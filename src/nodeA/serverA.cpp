#include <iostream>
#include <mutex>
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
using grpc::Status;
using dataflow::DataService;
using dataflow::Record;
using google::protobuf::Empty;

static const char* SHM_NAME = "/shm_A_to_B";
static const size_t SHM_SIZE = 500 * 1024 * 1024;
static std::mutex shm_mtx;
static char* shm_ptr = nullptr;
static size_t shm_off = 0;

class CallData {
public:
    CallData(DataService::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
        Proceed();
    }

    void Proceed(bool ok = true) {
        if (!ok) {
            delete this;
            return;
        }

        if (status_ == CREATE) {
            status_ = PROCESS;
            service_->RequestSendRecord(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new CallData(service_, cq_);
            
            // Write to shared memory
            {
                std::lock_guard<std::mutex> lock(shm_mtx);
                std::string row = request_.row_data() + "\n";
                if (shm_off + row.size() <= SHM_SIZE) {
                    memcpy(shm_ptr + shm_off, row.data(), row.size());
                    shm_off += row.size();
                    std::cout << "[A] Wrote row to SHM (" << shm_off << "/" << SHM_SIZE << ")\n";
                } else {
                    std::cerr << "[A] SHM full!\n";
                }
            }

            status_ = FINISH;
            responder_.Finish(Empty(), Status::OK, this);
        } else {
            delete this;
        }
    }

private:
    DataService::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;
    Record request_;
    Empty response_;
    grpc::ServerAsyncResponseWriter<Empty> responder_;

    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;
};

void HandleRpcs(DataService::AsyncService* service, ServerCompletionQueue* cq) {
    new CallData(service, cq);
    void* tag;
    bool ok;
    while (true) {
        if (!cq->Next(&tag, &ok)) {
            std::cerr << "[A] Completion queue shutdown\n";
            break;
        }
        static_cast<CallData*>(tag)->Proceed(ok);
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }

    NodeConfig cfg;
    if (!loadNodeConfig(argv[1], argv[2], cfg)) {
        std::cerr << "[A] Failed to load config\n";
        return 1;
    }

    // Setup shared memory
    shm_unlink(SHM_NAME);
    int fd = shm_open(SHM_NAME, O_CREAT|O_RDWR, 0666);
    if (fd < 0) {
        std::cerr << "[A] shm_open failed: " << strerror(errno) << "\n";
        return 1;
    }
    if (ftruncate(fd, SHM_SIZE) != 0) {
        std::cerr << "[A] ftruncate failed: " << strerror(errno) << "\n";
        close(fd);
        return 1;
    }
    shm_ptr = static_cast<char*>(mmap(nullptr, SHM_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0));
    close(fd);
    if (shm_ptr == MAP_FAILED) {
        std::cerr << "[A] mmap failed: " << strerror(errno) << "\n";
        return 1;
    }

    // Build server
    ServerBuilder builder;
    builder.AddListeningPort(cfg.listenAddress, grpc::InsecureServerCredentials());
    
    DataService::AsyncService service;
    builder.RegisterService(&service);
    
    std::unique_ptr<ServerCompletionQueue> cq = builder.AddCompletionQueue();
    std::unique_ptr<Server> server(builder.BuildAndStart());
    
    std::cout << "[A] Server listening on " << cfg.listenAddress << "\n";
    
    // Handle RPCs in a separate thread
    std::thread rpc_thread(HandleRpcs, &service, cq.get());
    
    server->Wait();
    rpc_thread.join();
    
    // Cleanup
    munmap(shm_ptr, SHM_SIZE);
    shm_unlink(SHM_NAME);
    
    return 0;
}