#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <atomic>
#include <csignal>
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

enum class TagType { HANDLER, FORWARD };

struct TagBase {
  virtual ~TagBase() = default;
  TagType type;
  TagBase(TagType t) : type(t) {}
};

class DHandler; // fwd

// Tag for forwarding to E
struct ForwardTag : public TagBase {
  ForwardTag(DHandler* h)
      : TagBase(TagType::FORWARD), handler(h) {}
  
  Record request;
  Empty response;
  ClientContext ctx;
  Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc;
  DHandler* handler;
};

// We'll store lines that D keeps
static std::vector<std::string> g_dataD;
static std::mutex g_mutexD;
static std::atomic<bool> g_exitFlagD(false);

// Summaries
static void printSummaryD(int);

// DHandler manages incoming calls for Node D
// D keeps 50% and forwards 50% to E
class DHandler : public TagBase {
public:
  enum CallStatus { CREATE, PROCESS, FORWARD, FINISH };

  DHandler(DataService::AsyncService* svc,
           ServerCompletionQueue* cq,
           DataService::Stub* stubE)
    : TagBase(TagType::HANDLER),
      service_(svc),
      cq_(cq),
      stubE_(stubE),
      responder_(&ctx_),
      status_(CREATE) {
    Proceed();
  }

  void Proceed() {
    if (status_ == CREATE) {
      // Request a new call
      status_ = PROCESS;
      service_->RequestSendRecord(&ctx_, &request_, &responder_, cq_, cq_, this);
    }
    else if (status_ == PROCESS) {
      // Spawn a new handler for next requests
      new DHandler(service_, cq_, stubE_);

      // We'll decide if we keep or forward
      // We'll do half keep, half forward
      static long long dLineCounter = 0;
      int modVal = dLineCounter % 2;
      dLineCounter++;

      if (modVal == 0) {
        // Keep
        {
          std::lock_guard<std::mutex> lock(g_mutexD);
          g_dataD.push_back(request_.row_data());
        }
        // Done
        status_ = FINISH;
        responder_.Finish(response_, Status::OK, this);
      } else {
        // Forward to E
        status_ = FORWARD;
        auto* ft = new ForwardTag(this);
        ft->request.set_row_data(request_.row_data());
        ft->rpc = stubE_->PrepareAsyncSendRecord(&ft->ctx, ft->request, cq_);
        ft->rpc->StartCall();
        ft->rpc->Finish(&ft->response, &ft->status, ft);
      }
    }
    else if (status_ == FORWARD) {
      // Forward done, respond to the original caller
      status_ = FINISH;
      responder_.Finish(response_, Status::OK, this);
    }
    else {
      // FINISH
      delete this;
    }
  }

  // Called when forwarding finishes
  void ForwardComplete() {
    Proceed();
  }

private:
  DataService::AsyncService* service_;
  ServerCompletionQueue* cq_;
  DataService::Stub* stubE_;

  ServerContext ctx_;
  Record request_;
  Empty response_;
  grpc::ServerAsyncResponseWriter<Empty> responder_;
  CallStatus status_;
};

static void signalHandlerD(int) {
  printSummaryD(0);
}

static void printSummaryD(int) {
  g_exitFlagD.store(true);

  std::lock_guard<std::mutex> lock(g_mutexD);
  size_t total = g_dataD.size();
  std::cout << "\n[D] ======= SUMMARY =======\n";
  std::cout << "[D] Total records: " << total << "\n";
  if (total > 0) {
    std::cout << "[D] First 5:\n";
    for (size_t i = 0; i < std::min<size_t>(5, total); i++) {
      std::cout << "  " << g_dataD[i] << "\n";
    }
    std::cout << "[D] Last 5:\n";
    for (size_t i = (total >= 5 ? total - 5 : 0); i < total; i++) {
      std::cout << "  " << g_dataD[i] << "\n";
    }
  }
  exit(0);
}

int main(int argc, char** argv) {
  signal(SIGINT, signalHandlerD);

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

  // Build stub to E
  auto stubE = DataService::NewStub(
      grpc::CreateChannel(cfg.neighbors["E"], grpc::InsecureChannelCredentials()));

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "[D] Async listening on " << cfg.listenAddress << "\n";

  // Start first handler
  new DHandler(&service, cq.get(), stubE.get());

  // Drain the CQ
  void* tag;
  bool ok;
  while (cq->Next(&tag, &ok)) {
    auto* base_tag = static_cast<TagBase*>(tag);
    if (!ok) {
      // Operation failed/cancelled
      delete base_tag;
    } else {
      if (base_tag->type == TagType::FORWARD) {
        // Forward finished
        auto* ft = static_cast<ForwardTag*>(base_tag);
        DHandler* handler = ft->handler;
        delete ft;
        handler->ForwardComplete();
      } else {
        // This is a DHandler
        static_cast<DHandler*>(base_tag)->Proceed();
      }
    }
  }

  server->Shutdown();
  cq->Shutdown();
  return 0;
}