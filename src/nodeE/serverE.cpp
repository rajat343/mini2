// src/nodeE/serverE_async.cpp
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
using grpc::Server; using grpc::ServerBuilder;
using grpc::ServerContext; using grpc::Status;
using dataflow::DataService; using dataflow::Record; using google::protobuf::Empty;
class EService : public DataService::Service {
  Status SendRecord(ServerContext*, const Record* r, Empty*) override {
    std::cout << "[E] " << r->row_data() << "\n";
    return Status::OK;
  }
};
int main(int argc,char**argv){
  NodeConfig cfg; loadNodeConfig(argv[1],argv[2],cfg);
  ServerBuilder b; b.AddListeningPort(cfg.listenAddress,grpc::InsecureServerCredentials());
  EService svc; b.RegisterService(&svc);
  auto s=b.BuildAndStart();
std::cout << "[E] Server listening on " << cfg.listenAddress << "\n";
  s->Wait();
}
