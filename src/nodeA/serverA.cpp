#include <iostream>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>

#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"

#include "read_config.h"  // for loadNodeConfig

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;
using dataflow::DataService;
using dataflow::Record;

static const char* SHM_NAME = "/shm_A_to_B";
static const size_t SHM_SIZE = 10 * 1024 * 1024;

std::mutex g_mutex;
char* g_shmPtr = nullptr;
size_t g_writeOffset = 0;

class NodeAServiceImpl final : public DataService::Service {
public:
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        std::lock_guard<std::mutex> lock(g_mutex);
        std::string row = request->row_data() + "\n";
        if (g_writeOffset + row.size() < SHM_SIZE) {
            memcpy(g_shmPtr + g_writeOffset, row.data(), row.size());
            g_writeOffset += row.size();
        }
        return Status::OK;
    }
};

void RunNodeA(const std::string& jsonFile, const std::string& nodeName) {
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "Failed to load config for " << nodeName << std::endl;
        return;
    }

    // Shared memory
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    ftruncate(fd, SHM_SIZE);
    g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    // gRPC server
    NodeAServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[A] Listening on " << config.listenAddress << std::endl;
    server->Wait();

    munmap(g_shmPtr, SHM_SIZE);
    shm_unlink(SHM_NAME);
}

int main(int argc, char** argv) {
    // usage: serverA <path_to_json> A
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    std::string jsonFile = argv[1];
    std::string nodeName = argv[2];
    RunNodeA(jsonFile, nodeName);
    return 0;
}
