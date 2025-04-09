// src/nodeC/serverC_async.cpp
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
using grpc::Server; using grpc::ServerBuilder;
using grpc::ServerContext; using grpc::Status;
using dataflow::DataService; using dataflow::Record; using google::protobuf::Empty;
class CService : public DataService::Service {
  Status SendRecord(ServerContext*, const Record* r, Empty*) override {
    std::cout << "[C] " << r->row_data() << "\n";
    return Status::OK;
  }
};
int main(int argc,char**argv){
  struct rlimit limits;
  limits.rlim_cur = 65535;  // Increase soft limit
  limits.rlim_max = 65535;  // Increase hard limit
  setrlimit(RLIMIT_NOFILE, &limits);

  NodeConfig cfg; loadNodeConfig(argv[1],argv[2],cfg);
  ServerBuilder b; b.AddListeningPort(cfg.listenAddress,grpc::InsecureServerCredentials());
  CService svc; b.RegisterService(&svc);
  auto s=b.BuildAndStart();
   std::cout << "[C] Server listening on " << cfg.listenAddress << "\n";
   s->Wait();
}
