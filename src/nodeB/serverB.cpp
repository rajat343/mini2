// src/nodeB/serverB_async.cpp
#include <iostream>
#include <thread>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"

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

// Tag struct for each async call
struct CallData {
  Record request;
  Empty response;
  ClientContext ctx;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc;
};

// Async forwarder without external header
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
    // We don't actually serve external RPCs on B; this is just to keep the CQ alive.
    // If B had incoming RPCs, we'd call RequestSendRecord here.
  }

private:
  DataService::AsyncService* service_;
  ServerCompletionQueue* cq_;
};

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
    int lineCounter = 0;
    int errorCounter = 0;
    const int LOG_INTERVAL = 1000;

    while (true) {
        try {
            while (readOff < SHM_SIZE && ptr[readOff]) {
                if (ptr[readOff] == '\n') {
                    std::string line(ptr + last, readOff - last);
                    
                    // Forward to both C and D
                    fwdC->forward(line);
                    fwdD->forward(line);
                    
                    lineCounter++;
                    if (lineCounter % LOG_INTERVAL == 0) {
                        std::cout << "[B] Processed " << lineCounter << " lines (errors: " 
                                  << errorCounter << ")\n";
                    }
                    
                    readOff++;
                    last = readOff;
                } else {
                    readOff++;
                }
            }

            // Reset if we've reached the end of buffer
            if (readOff >= SHM_SIZE) {
                readOff = 0;
                last = 0;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } catch (const std::exception& e) {
            errorCounter++;
            std::cerr << "[B] Error processing line " << lineCounter 
                      << ": " << e.what() << "\n";
            // Reset to last known good position
            readOff = last;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Cleanup (though this thread runs indefinitely)
    munmap(ptr, SHM_SIZE);
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
    return 1;
  }
  NodeConfig cfg;
  if (!loadNodeConfig(argv[1], argv[2], cfg)) {
    std::cerr << "[B] Failed to load config\n";
    return 1;
  }

  ServerBuilder builder;
  builder.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());
  DataService::AsyncService service;
  builder.RegisterService(&service);
  auto cq = builder.AddCompletionQueue();
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "[B] Async listening on " << cfg.listenAddress << "\n";

  // Build stubs
  auto stubC = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["C"], grpc::InsecureChannelCredentials()));
  auto stubD = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["D"], grpc::InsecureChannelCredentials()));

  AsyncForwarder fwdC(stubC.get(), cq.get());
  AsyncForwarder fwdD(stubD.get(), cq.get());

  // Start shared‑memory reader
  std::thread(readerThread, &fwdC, &fwdD).detach();

  // Drain the completion queue to delete CallData
  void* tag;
  bool ok;
  while (cq->Next(&tag, &ok)) {
    delete static_cast<CallData*>(tag);
  }

  server->Shutdown();
  return 0;
}
