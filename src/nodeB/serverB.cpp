#include <iostream>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"

#include "read_config.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;
using google::protobuf::Empty;
using dataflow::DataService;
using dataflow::Record;

static const char* SHM_NAME = "/shm_A_to_B";
static const size_t SHM_SIZE = 10 * 1024 * 1024;

std::mutex g_mutex;
char* g_shmPtr = nullptr;
size_t g_readOffset = 0;

// We'll store neighbor stubs in a map
std::map<std::string, std::unique_ptr<DataService::Stub>> g_stubs;

static const float KEEP_PERCENT      = 0.25f;
static const float SEND_TO_C_PERCENT = 0.25f;
static const float SEND_TO_D_PERCENT = 0.50f;

class NodeBServiceImpl final : public DataService::Service {
public:
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        // If B also needs to receive data from somewhere else, handle it here
        return Status::OK;
    }
};

void forwardTo(const std::string& neighborName, const std::string& data) {
    auto it = g_stubs.find(neighborName);
    if (it == g_stubs.end()) {
        std::cerr << "[B] No stub for neighbor " << neighborName << "\n";
        return;
    }
    Record record;
    record.set_row_data(data);
    Empty e;
    ClientContext ctx;
    it->second->SendRecord(&ctx, record, &e);
}

void readAndDistribute() {
    std::vector<std::string> lines;
    size_t lastOffset = 0;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            while (g_readOffset < SHM_SIZE) {
                if (g_shmPtr[g_readOffset] == '\n') {
                    size_t length = g_readOffset - lastOffset;
                    std::string line(g_shmPtr + lastOffset, length);
                    lines.push_back(line);
                    g_readOffset++;
                    lastOffset = g_readOffset;
                } else if (g_shmPtr[g_readOffset] == '\0') {
                    // no more data
                    break;
                } else {
                    g_readOffset++;
                }
            }
        }

        // Distribute if we have new lines
        if (!lines.empty()) {
            size_t total    = lines.size();
            size_t keepCnt  = (size_t)(total * KEEP_PERCENT);
            size_t cCnt     = (size_t)(total * SEND_TO_C_PERCENT);
            size_t dCnt     = total - keepCnt - cCnt;

            // B keeps keepCnt
            for (size_t i = 0; i < keepCnt; i++) {
                std::cout << "[B] Keeping: " << lines[i] << std::endl;
            }

            // We expect neighbor "C" and "D" if they exist
            // We'll get the keys from g_stubs:
            // Or we can just do a quick approach:
            if (g_stubs.find("C") != g_stubs.end()) {
                for (size_t i = keepCnt; i < keepCnt + cCnt; i++) {
                    forwardTo("C", lines[i]);
                }
            }
            if (g_stubs.find("D") != g_stubs.end()) {
                for (size_t i = keepCnt + cCnt; i < total; i++) {
                    forwardTo("D", lines[i]);
                }
            }
            lines.clear();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void RunNodeB(const std::string& jsonFile, const std::string& nodeName) {
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "Failed to load config for node B\n";
        return;
    }

    // Attach shared memory
    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    // Build stubs to neighbors (C, D)
    for (const auto& kv : config.neighbors) {
        const std::string& neighborName = kv.first;   // e.g. "C"
        const std::string& neighborAddr = kv.second;  // e.g. "192.168.0.2:50053"
        g_stubs[neighborName] = 
            DataService::NewStub(grpc::CreateChannel(neighborAddr, grpc::InsecureChannelCredentials()));
        std::cout << "[B] Created stub for neighbor: " << neighborName 
                  << " at " << neighborAddr << std::endl;
    }

    // Start background thread
    std::thread distThread(readAndDistribute);
    distThread.detach();

    // Possibly run a server if B also receives external calls
    NodeBServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "[B] Listening on " << config.listenAddress << std::endl;
    server->Wait();

    munmap(g_shmPtr, SHM_SIZE);
    shm_unlink(SHM_NAME);
}

int main(int argc, char** argv) {
    // usage: serverB <config.json> B
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeB(argv[1], argv[2]);
    return 0;
}
