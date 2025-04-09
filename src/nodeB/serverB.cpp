#include <iostream>
#include <thread>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <csignal>
#include <atomic>
#include <vector>
#include <mutex>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::ClientContext;
using grpc::Status;
using dataflow::DataService;
using dataflow::Record;
using google::protobuf::Empty;

static const char* SHM_NAME = "/shm_A_to_B";
static const size_t SHM_SIZE = 500 * 1024 * 1024;

// We'll keep track of lines that B "keeps" in a global vector
static std::vector<std::string> g_keptDataB;
static std::mutex g_dataMutexB;
static std::atomic<bool> g_exitFlag(false);

// Forward declaration for cleanup
static void printSummaryAndExitB(int);

// Tag struct for each async call to C or D
struct CallData {
  Record request;
  Empty response;
  ClientContext ctx;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc;
};

// Async forwarder: calls Node C or Node D asynchronously
class AsyncForwarder {
  DataService::Stub* stub_;
  ServerCompletionQueue* cq_;

public:
  AsyncForwarder(DataService::Stub* stub, ServerCompletionQueue* cq)
    : stub_(stub), cq_(cq) {}

  void forward(const std::string& payload) {
    auto* call = new CallData;
    call->request.set_row_data(payload);

    call->rpc = stub_->PrepareAsyncSendRecord(&call->ctx, call->request, cq_);
    call->rpc->StartCall();
    call->rpc->Finish(&call->response, &call->status, call);
  }
};

class NodeBService {
public:
  NodeBService(DataService::AsyncService* svc, ServerCompletionQueue* cq)
    : service_(svc), cq_(cq) {
    proceed();
  }

  void proceed() {
    // We don't actually serve external RPC calls on B in this design,
    // but we must keep the service/cq alive. If B also received gRPC
    // calls, we'd do RequestSendRecord here.
  }

private:
  DataService::AsyncService* service_;
  ServerCompletionQueue* cq_;
};

// We'll use a thread to read from shared memory, distributing data as specified:
//  - 25% keep in B
//  - 25% send to C
//  - 50% send to D
void readerThread(AsyncForwarder* fwdC, AsyncForwarder* fwdD) {
    int fd = shm_open(SHM_NAME, O_RDONLY, 0666);
    if (fd < 0) {
        std::cerr << "[B] shm_open failed: " << strerror(errno) << "\n";
        return;
    }

    char* ptr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "[B] mmap failed: " << strerror(errno) << "\n";
        return;
    }

    std::cout << "[B] Reader thread started successfully\n";

    size_t readOff = 0, last = 0;
    long long lineCounter = 0;
    const int LOG_INTERVAL = 1000;

    while (!g_exitFlag.load()) {
        // If we've read everything up to a null, check for new data
        while (readOff < SHM_SIZE && ptr[readOff]) {
            if (ptr[readOff] == '\n') {
                std::string line(ptr + last, readOff - last);
                
                // Decide distribution
                // We want 25% keep, 25% C, 50% D
                // E.g. use lineCounter % 4:
                // 0 -> keep
                // 1 -> C
                // 2,3 -> D
                int modVal = lineCounter % 4;
                if (modVal == 0) {
                    // keep in B
                    {
                      std::lock_guard<std::mutex> lock(g_dataMutexB);
                      g_keptDataB.push_back(line);
                    }
                } else if (modVal == 1) {
                    // send to C
                    fwdC->forward(line);
                } else {
                    // send to D
                    fwdD->forward(line);
                }

                lineCounter++;
                if (lineCounter % LOG_INTERVAL == 0) {
                    std::cout << "[B] Processed " << lineCounter << " lines so far\n";
                }

                readOff++;
                last = readOff;
            } else {
                readOff++;
            }
        }

        if (readOff >= SHM_SIZE) {
            // If we reached end, wrap around
            readOff = 0;
            last = 0;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    munmap(ptr, SHM_SIZE);
    std::cout << "[B] Reader thread exit\n";
}

// Handler to print summary on Ctrl+C
static void signalHandlerB(int) {
  printSummaryAndExitB(0);
}

static void printSummaryAndExitB(int) {
  g_exitFlag.store(true);

  // Print B's first 5, last 5, total
  std::vector<std::string> localCopy;
  {
    std::lock_guard<std::mutex> lock(g_dataMutexB);
    localCopy = g_keptDataB;
  }

  size_t total = localCopy.size();
  std::cout << "\n[B] ======= SUMMARY =======\n";
  std::cout << "[B] Total records kept: " << total << "\n";
  if (total > 0) {
    std::cout << "[B] First 5:\n";
    for (size_t i = 0; i < std::min<size_t>(5, total); i++) {
      std::cout << "  " << localCopy[i] << "\n";
    }
    std::cout << "[B] Last 5:\n";
    for (size_t i = (total >= 5 ? total - 5 : 0); i < total; i++) {
      std::cout << "  " << localCopy[i] << "\n";
    }
  }

  exit(0); // Force exit
}

int main(int argc, char** argv) {
  // Setup signal handler for Ctrl+C
  signal(SIGINT, signalHandlerB);

  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
    return 1;
  }
  NodeConfig cfg;
  if (!loadNodeConfig(argv[1], argv[2], cfg)) {
    std::cerr << "[B] Failed to load config\n";
    return 1;
  }

  // Build async server (even though B doesn't really receive external requests)
  ServerBuilder builder;
  builder.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());
  DataService::AsyncService service;
  builder.RegisterService(&service);
  auto cq = builder.AddCompletionQueue();
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "[B] Async listening on " << cfg.listenAddress << "\n";

  // Build stubs for C and D
  auto stubC = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["C"], grpc::InsecureChannelCredentials()));
  auto stubD = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["D"], grpc::InsecureChannelCredentials()));

  AsyncForwarder fwdC(stubC.get(), cq.get());
  AsyncForwarder fwdD(stubD.get(), cq.get());

  // Create "service" to keep the completion queue alive
  new NodeBService(&service, cq.get());

  // Start shared memory reading thread
  std::thread(readerThread, &fwdC, &fwdD).detach();

  // Drain the completion queue (not truly used here except for async calls)
  void* tag;
  bool ok;
  while (cq->Next(&tag, &ok)) {
    // All calls to C or D we created come back here
    // We just delete them
    delete static_cast<CallData*>(tag);
  }

  server->Shutdown();
  return 0;
}