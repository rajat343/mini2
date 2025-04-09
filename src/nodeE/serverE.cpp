#include <iostream>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <vector>
#include <mutex>
#include <atomic>
#include <csignal>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using dataflow::DataService;
using dataflow::Record;
using google::protobuf::Empty;

// We'll store lines that E receives
static std::vector<std::string> g_dataE;
static std::mutex g_mutexE;
static std::atomic<bool> g_exitFlagE(false);

static void printSummaryE(int);

class EService : public DataService::Service {
  Status SendRecord(ServerContext*, const Record* r, Empty*) override {
    // Keep the data
    {
      std::lock_guard<std::mutex> lock(g_mutexE);
      g_dataE.push_back(r->row_data());
    }
    return Status::OK;
  }
};

static void signalHandlerE(int) {
  printSummaryE(0);
}

static void printSummaryE(int) {
  g_exitFlagE.store(true);

  std::lock_guard<std::mutex> lock(g_mutexE);
  size_t total = g_dataE.size();
  std::cout << "\n[E] ======= SUMMARY =======\n";
  std::cout << "[E] Total records: " << total << "\n";
  if (total > 0) {
    std::cout << "[E] First 5:\n";
    for (size_t i = 0; i < std::min<size_t>(5, total); i++) {
      std::cout << "  " << g_dataE[i] << "\n";
    }
    std::cout << "[E] Last 5:\n";
    for (size_t i = (total >= 5 ? total - 5 : 0); i < total; i++) {
      std::cout << "  " << g_dataE[i] << "\n";
    }
  }
  exit(0);
}

int main(int argc,char**argv){
  // Ctrl+C => summary
  signal(SIGINT, signalHandlerE);

  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
    return 1;
  }
  NodeConfig cfg;
  if (!loadNodeConfig(argv[1], argv[2], cfg)) {
    std::cerr << "[E] Failed to load config\n";
    return 1;
  }

  ServerBuilder b;
  b.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());
  EService svc;
  b.RegisterService(&svc);
  auto s = b.BuildAndStart();
  std::cout << "[E] Server listening on " << cfg.listenAddress << "\n";

  s->Wait();
  return 0;
}