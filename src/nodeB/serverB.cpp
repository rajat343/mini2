// #include <iostream>
// #include <memory>
// #include <string>
// #include <sys/mman.h>
// #include <sys/stat.h>
// #include <fcntl.h>
// #include <unistd.h>
// #include <mutex>
// #include <thread>
// #include <chrono>

// #include <grpcpp/grpcpp.h>
// #include "data.pb.h"
// #include "data.grpc.pb.h"

// #include "read_config.h"

// using grpc::Server;
// using grpc::ServerBuilder;
// using grpc::ServerContext;
// using grpc::ClientContext;
// using grpc::Status;
// using google::protobuf::Empty;
// using dataflow::DataService;
// using dataflow::Record;

// static const char* SHM_NAME = "/shm_A_to_B";
// static const size_t SHM_SIZE = 10 * 1024 * 1024;

// std::mutex g_mutex;
// char* g_shmPtr = nullptr;
// size_t g_readOffset = 0;

// // We'll store neighbor stubs in a map
// std::map<std::string, std::unique_ptr<DataService::Stub>> g_stubs;

// static const float KEEP_PERCENT      = 0.25f;
// static const float SEND_TO_C_PERCENT = 0.25f;
// static const float SEND_TO_D_PERCENT = 0.50f;

// class NodeBServiceImpl final : public DataService::Service {
// public:
//     Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
//         // If B also needs to receive data from somewhere else, handle it here
//         return Status::OK;
//     }
// };

// void forwardTo(const std::string& neighborName, const std::string& data) {
//     auto it = g_stubs.find(neighborName);
//     if (it == g_stubs.end()) {
//         std::cerr << "[B] No stub for neighbor " << neighborName << "\n";
//         return;
//     }
//     // LOG: Indicate we are about to forward data
//     std::cout << "[B] Forwarding to " << neighborName << ": " << data << std::endl;

//     Record record;
//     record.set_row_data(data);
//     Empty e;
//     ClientContext ctx;
//     it->second->SendRecord(&ctx, record, &e);
// }

// void readAndDistribute() {
//     std::vector<std::string> lines;
//     size_t lastOffset = 0;

//     // LOG: Indicate we started the readAndDistribute thread
//     std::cout << "[B] readAndDistribute() thread started, ready to read shared memory.\n";

//     while (true) {
//         {
//             std::lock_guard<std::mutex> lock(g_mutex);

//             // LOG: We can optionally log the current readOffset
//             // std::cout << "[B] Current g_readOffset: " << g_readOffset << std::endl;

//             while (g_readOffset < SHM_SIZE) {
//                 if (g_shmPtr[g_readOffset] == '\n') {
//                     size_t length = g_readOffset - lastOffset;
//                     std::string line(g_shmPtr + lastOffset, length);

//                     // LOG: Printing every line read (can be verbose!)
//                     std::cout << "[B] Read line from shared memory: " << line << std::endl;

//                     lines.push_back(line);
//                     g_readOffset++;
//                     lastOffset = g_readOffset;
//                 } else if (g_shmPtr[g_readOffset] == '\0') {
//                     // no more data
//                     break;
//                 } else {
//                     g_readOffset++;
//                 }
//             }
//         }

//         // LOG: We can optionally log how many lines are pending distribution
//         // std::cout << "[B] lines.size() = " << lines.size() << std::endl;

//         if (!lines.empty()) {
//             size_t total = lines.size();
//             size_t keepCnt = static_cast<size_t>(total * KEEP_PERCENT);
//             size_t cCnt    = static_cast<size_t>(total * SEND_TO_C_PERCENT);
//             size_t dCnt    = total - keepCnt - cCnt;

//             std::cout << "[B] Distributing " << total << " new lines: "
//                       << keepCnt << " kept, " << cCnt << " to C, " << dCnt << " to D.\n";

//             // B keeps keepCnt
//             for (size_t i = 0; i < keepCnt; i++) {
//                 std::cout << "[B] Keeping: " << lines[i] << std::endl;
//             }

//             // send cCount lines to C
//             if (g_stubs.find("C") != g_stubs.end()) {
//                 for (size_t i = keepCnt; i < keepCnt + cCnt; i++) {
//                     forwardTo("C", lines[i]);
//                 }
//             } else {
//                 // LOG: if no "C" neighbor in g_stubs
//                 std::cout << "[B] No neighbor C in config, skipping.\n";
//             }

//             // send dCount lines to D
//             if (g_stubs.find("D") != g_stubs.end()) {
//                 for (size_t i = keepCnt + cCnt; i < total; i++) {
//                     forwardTo("D", lines[i]);
//                 }
//             } else {
//                 // LOG: if no "D" neighbor in g_stubs
//                 std::cout << "[B] No neighbor D in config, skipping.\n";
//             }

//             lines.clear();
//         }

//         std::this_thread::sleep_for(std::chrono::milliseconds(200));
//     }
// }

// void RunNodeB(const std::string& jsonFile, const std::string& nodeName) {
//     // LOG: Show which config file and node name we're using
//     std::cout << "[B] RunNodeB starting with config: " << jsonFile
//               << ", nodeName: " << nodeName << std::endl;

//     NodeConfig config;
//     if (!loadNodeConfig(jsonFile, nodeName, config)) {
//         std::cerr << "[B] Failed to load config for node B\n";
//         return;
//     }

//     // LOG: Show the address this node will listen on
//     std::cout << "[B] My listen_address from config is: " << config.listenAddress << std::endl;

//     // Attach shared memory
//     int fd = shm_open(SHM_NAME, O_RDWR, 0666);
//     if (fd < 0) {
//         std::cerr << "[B] shm_open failed with error\n";
//         return;
//     }
//     g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
//     close(fd);

//     if (g_shmPtr == MAP_FAILED) {
//         std::cerr << "[B] mmap failed, cannot attach shared memory.\n";
//         return;
//     }
//     // LOG: confirm we attached shared memory successfully
//     std::cout << "[B] Successfully attached shared memory: " << SHM_NAME << std::endl;

//     // Build stubs to neighbors (C, D)
//     for (const auto& kv : config.neighbors) {
//         const std::string& neighborName = kv.first;   // e.g. "C"
//         const std::string& neighborAddr = kv.second;  // e.g. "192.168.0.2:50053"
//         g_stubs[neighborName] =
//             DataService::NewStub(grpc::CreateChannel(neighborAddr, grpc::InsecureChannelCredentials()));

//         // LOG: confirm we created a stub
//         std::cout << "[B] Created stub for neighbor: " << neighborName
//                   << " at " << neighborAddr << std::endl;
//     }

//     // Start background thread
//     std::thread distThread(readAndDistribute);
//     distThread.detach();

//     // Possibly run a server if B also receives external calls
//     NodeBServiceImpl service;
//     ServerBuilder builder;
//     builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
//     builder.RegisterService(&service);

//     auto server = builder.BuildAndStart();
//     // LOG: confirm we're now listening
//     std::cout << "[B] Listening on " << config.listenAddress << std::endl;
//     server->Wait();

//     // Cleanup
//     munmap(g_shmPtr, SHM_SIZE);
//     shm_unlink(SHM_NAME);
// }

// int main(int argc, char** argv) {
//     // usage: serverB <config.json> B
//     if (argc < 3) {
//         std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
//         return 1;
//     }
//     RunNodeB(argv[1], argv[2]);
//     return 0;
// }




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
#include <map>
#include <vector>
#include <csignal>           // <-- for std::signal and SIGINT

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
static const size_t SHM_SIZE = 500 * 1024 * 1024;

std::mutex g_mutex;
char* g_shmPtr = nullptr;
size_t g_readOffset = 0;
std::map<std::string, std::unique_ptr<DataService::Stub>> g_stubs;

static const float KEEP_PERCENT = 0.25f;
static const float SEND_TO_C_PERCENT = 0.25f;
static const float SEND_TO_D_PERCENT = 0.50f;

// Trackers for final summary
std::vector<std::string> g_keptRows;
std::vector<std::string> g_sentToCRows;
std::vector<std::string> g_sentToDRows;

// Make the gRPC server a static/global so signal handler can call Shutdown()
static std::unique_ptr<Server> g_server;

// Signal handler for graceful shutdown
void handleSigint(int /* signum */) {
    if (g_server) {
        std::cout << "[B] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown(); // This unblocks server->Wait()
    }
}

class NodeBServiceImpl final : public DataService::Service {
public:
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        // We don't do anything in B's SendRecord, since data is coming from A->B via shared memory
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
    bool isFirstLine = true;

    while (true) {
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            while (g_readOffset < SHM_SIZE) {
                if (g_shmPtr[g_readOffset] == '\n') {
                    size_t length = g_readOffset - lastOffset;
                    std::string line(g_shmPtr + lastOffset, length);

                    if (isFirstLine) {
                        std::cout << "[B] FIRST LINE READ: " << line << "\n";
                        isFirstLine = false;
                    }

                    lines.push_back(line);
                    g_readOffset++;
                    lastOffset = g_readOffset;
                }
                else if (g_shmPtr[g_readOffset] == '\0') {
                    // no more data in the shared memory
                    break;
                }
                else {
                    g_readOffset++;
                }
            }
        }

        if (!lines.empty()) {
            size_t total = lines.size();
            size_t keepCnt = static_cast<size_t>(total * KEEP_PERCENT);
            size_t cCnt = static_cast<size_t>(total * SEND_TO_C_PERCENT);
            size_t dCnt = total - keepCnt - cCnt;

            // Keep rows
            for (size_t i = 0; i < keepCnt; i++) {
                  g_keptRows.push_back(lines[i]);
                std::cout << "[B] ADDED to g_keptRows: \"" << lines[i]
              << "\" (total now " << g_keptRows.size() << ")\n";
              
            }

            // Send to C
            for (size_t i = keepCnt; i < keepCnt + cCnt; i++) {
                g_sentToCRows.push_back(lines[i]);
                // Log the line that goes to C
             std::cout << "[B] ADDED to g_sentToCRows: \"" << lines[i]
              << "\" (total now " << g_sentToCRows.size() << ")\n";
                forwardTo("C", lines[i]);
            }

            // Send to D
            for (size_t i = keepCnt + cCnt; i < total; i++) {
                g_sentToDRows.push_back(lines[i]);
                std::cout << "[B] ADDED to g_sentToDRows: \"" << lines[i]
              << "\" (total now " << g_sentToDRows.size() << ")\n";
                forwardTo("D", lines[i]);
                //forwardTo("D", lines[i]);
            }

            lines.clear();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void RunNodeB(const std::string& jsonFile, const std::string& nodeName) {
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[B] Failed to load config.\n";
        return;
    }

    // Attach shared memory
    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (fd < 0) {
        std::cerr << "[B] shm_open failed.\n";
        return;
    }
    g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (g_shmPtr == MAP_FAILED) {
        std::cerr << "[B] mmap failed.\n";
        return;
    }

    // Build stubs to neighbors
    for (const auto& kv : config.neighbors) {
        g_stubs[kv.first] = DataService::NewStub(
            grpc::CreateChannel(kv.second, grpc::InsecureChannelCredentials())
        );
    }

    // Start background thread to read from shared memory
    std::thread distThread(readAndDistribute);
    distThread.detach();

    // Start gRPC server, but store it in a global so SIGINT can shut it down
    NodeBServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Install our signal handler
    std::signal(SIGINT, handleSigint);

    g_server = builder.BuildAndStart();
    std::cout << "[B] Listening on " << config.listenAddress << "\n";
    // Blocks until Shutdown() is called
    g_server->Wait();

    // After Wait() returns, we can do final summary + cleanup
    std::cout << "\n[B] FINAL SUMMARY:\n";
    if (!g_keptRows.empty()) {
        std::cout << "  - KEPT: " << g_keptRows.size() << " rows\n"
                  << "    FIRST: " << g_keptRows.front() << "\n"
                  << "    LAST: " << g_keptRows.back() << "\n";
    }
    if (!g_sentToCRows.empty()) {
        std::cout << "  - SENT TO C: " << g_sentToCRows.size() << " rows\n"
                  << "    FIRST: " << g_sentToCRows.front() << "\n"
                  << "    LAST: " << g_sentToCRows.back() << "\n";
    }
    if (!g_sentToDRows.empty()) {
        std::cout << "  - SENT TO D: " << g_sentToDRows.size() << " rows\n"
                  << "    FIRST: " << g_sentToDRows.front() << "\n"
                  << "    LAST: " << g_sentToDRows.back() << "\n";
    }

    // cleanup shared memory if you like
    munmap(g_shmPtr, SHM_SIZE);
    // shm_unlink(SHM_NAME); // optional if you want to remove it
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeB(argv[1], argv[2]);
    return 0;
}
