#include <iostream>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <csignal>
#include <vector>
#include <mutex>
#include <atomic>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using dataflow::DataService;
using dataflow::Record;
using google::protobuf::Empty;

// We'll store lines that C receives
static std::vector<std::string> g_dataC;
static std::mutex g_mutexC;
static std::atomic<bool> g_exitFlagC(false);

static void printSummaryC(int);

class CService : public DataService::Service {
  Status SendRecord(ServerContext*, const Record* r, Empty*) override {
    // Store the incoming record
    {
      std::lock_guard<std::mutex> lock(g_mutexC);
      g_dataC.push_back(r->row_data());
    }
    return Status::OK;
  }
};

static void signalHandlerC(int) {
  printSummaryC(0);
}

static void printSummaryC(int) {
  g_exitFlagC.store(true);

  std::lock_guard<std::mutex> lock(g_mutexC);
  size_t total = g_dataC.size();
  std::cout << "\n[C] ======= SUMMARY =======\n";
  std::cout << "[C] Total records: " << total << "\n";
  if (total > 0) {
    std::cout << "[C] First 5:\n";
    for (size_t i = 0; i < std::min<size_t>(5, total); i++) {
      std::cout << "  " << g_dataC[i] << "\n";
    }
    std::cout << "[C] Last 5:\n";
    for (size_t i = (total >= 5 ? total - 5 : 0); i < total; i++) {
      std::cout << "  " << g_dataC[i] << "\n";
    }
  }
  exit(0);
}

int main(int argc,char**argv){
  // Handle Ctrl+C to print summary
  signal(SIGINT, signalHandlerC);

  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
    return 1;
  }

  NodeConfig cfg;
  if (!loadNodeConfig(argv[1], argv[2], cfg)) {
    std::cerr << "[C] Failed to load config\n";
    return 1;
  }

  ServerBuilder b;
  b.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());
  CService svc;
  b.RegisterService(&svc);
  auto s = b.BuildAndStart();
  std::cout << "[C] Server listening on " << cfg.listenAddress << "\n";

  s->Wait();
  return 0;
}