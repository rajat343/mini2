#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <atomic>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::ClientContext;
using grpc::Status;
using dataflow::DataService;
using dataflow::Record;
using google::protobuf::Empty;

std::atomic<int> active_requests(0);
const int MAX_CONCURRENT = 1000;

// Define tag types for different operations
enum class TagType { HANDLER, FORWARD };

// Base class for all tags
struct TagBase {
  virtual ~TagBase() = default;
  TagType type;
  
  TagBase(TagType t) : type(t) {}
};

// Forward declaration for handler
class DHandler;

// Tag for forwarding data to node E
struct ForwardTag : public TagBase {
  ForwardTag(DHandler* h) : TagBase(TagType::FORWARD), handler(h) {}
  
  Record request;
  Empty response;
  ClientContext ctx;
  Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc;
  DHandler* handler;
};

class DHandler : public TagBase {
public:
  DHandler(DataService::AsyncService* svc,
           ServerCompletionQueue* cq,
           DataService::Stub* stub_e)
    : TagBase(TagType::HANDLER), 
      service_(svc), 
      cq_(cq), 
      stub_e_(stub_e), 
      responder_(&ctx_), 
      status_(CREATE) {
    Proceed();
  }

  void Proceed() {
    if (status_ == CREATE) {
      // Request a new call
      status_ = PROCESS;
      service_->RequestSendRecord(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
      // Create a new handler to service the next request
      new DHandler(service_, cq_, stub_e_);
      
      // Check if we've reached maximum concurrent requests
      if (active_requests >= MAX_CONCURRENT) {
        status_ = FINISH;
        responder_.FinishWithError(
            Status(grpc::RESOURCE_EXHAUSTED, "Too many requests"), this);
        return;
      }
      
      // Forward the request to node E
      active_requests++;
      status_ = FORWARD;
      
      auto* fd = new ForwardTag(this);
      fd->request.set_row_data(request_.row_data());
      fd->rpc = stub_e_->PrepareAsyncSendRecord(&fd->ctx, fd->request, cq_);
      fd->rpc->StartCall();
      fd->rpc->Finish(&fd->response, &fd->status, fd);
    } else if (status_ == FORWARD) {
      // Forward completed, finish our response
      status_ = FINISH;
      responder_.Finish(response_, Status::OK, this);
    } else {
      // We're done, decrement active requests and clean up
      active_requests--;
      delete this;
    }
  }
  
  // Called when forwarding is complete
  void ForwardComplete() {
    Proceed(); // Move to next state
  }

  enum CallStatus { CREATE, PROCESS, FORWARD, FINISH };
  CallStatus status_;

private:
  DataService::AsyncService* service_;
  ServerCompletionQueue* cq_;
  DataService::Stub* stub_e_;
  ServerContext ctx_;
  Record request_;
  Empty response_;
  grpc::ServerAsyncResponseWriter<Empty> responder_;
};

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
    return 1;
  }
  NodeConfig cfg;
  if (!loadNodeConfig(argv[1], argv[2], cfg)) {
    std::cerr << "[D] Failed to load config\n";
    return 1;
  }

  ServerBuilder builder;
  builder.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());

  DataService::AsyncService service;
  builder.RegisterService(&service);
  auto cq = builder.AddCompletionQueue();

  // Create stub to E
  auto stub_e = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["E"], grpc::InsecureChannelCredentials()));

  // Build and start server
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "[D] Async listening on " << cfg.listenAddress << "\n";

  // Spawn first handler
  new DHandler(&service, cq.get(), stub_e.get());

  // Completion queue loop
  void* tag;
  bool ok;
  while (cq->Next(&tag, &ok)) {
    auto* base_tag = static_cast<TagBase*>(tag);
    
    if (ok) {
      // Process based on tag type
      if (base_tag->type == TagType::FORWARD) {
        // This is a completed forward call
        auto* forward_tag = static_cast<ForwardTag*>(base_tag);
        DHandler* handler = forward_tag->handler;
        delete forward_tag;  // Clean up ForwardTag
        handler->ForwardComplete();  // Move the handler to the next state
      } else {
        // This is a DHandler tag
        static_cast<DHandler*>(base_tag)->Proceed();
      }
    } else {
      // RPC was cancelled or failed
      delete base_tag; // Clean up the tag object
    }
  }

  server->Shutdown();
  cq->Shutdown();
  return 0;
}