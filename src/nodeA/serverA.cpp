// #include <iostream>
// #include <memory>
// #include <string>
// #include <sys/mman.h>
// #include <sys/stat.h>
// #include <fcntl.h>
// #include <unistd.h>
// #include <mutex>

// #include <grpcpp/grpcpp.h>
// #include "data.pb.h"
// #include "data.grpc.pb.h"

// #include "read_config.h"  // for loadNodeConfig

// using grpc::Server;
// using grpc::ServerBuilder;
// using grpc::ServerContext;
// using grpc::Status;
// using google::protobuf::Empty;
// using dataflow::DataService;
// using dataflow::Record;

// static const char* SHM_NAME = "/shm_A_to_B";
// static const size_t SHM_SIZE = 500 * 1024 * 1024;
// size_t g_rowCount = 0;

// // Global resources for shared memory
// std::mutex g_mutex;
// char* g_shmPtr = nullptr;
// size_t g_writeOffset = 0;

// class NodeAServiceImpl final : public DataService::Service {
// public:
//     // LOG: On each SendRecord call, we print to see if data is received
//     Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
//     std::cout << "[A] Received row: " << request->row_data() << std::endl;

//     std::lock_guard<std::mutex> lock(g_mutex);
//     std::string row = request->row_data() + "\n";

//     std::cout << "[A] Current g_writeOffset: " << g_writeOffset
//               << ", row size: " << row.size() << std::endl;

//     if (g_writeOffset + row.size() < SHM_SIZE) {
//         memcpy(g_shmPtr + g_writeOffset, row.data(), row.size());
//         g_writeOffset += row.size();
//         g_rowCount++;  // Increment the row counter
//         std::cout << "[A] Wrote row to shared memory. New g_writeOffset: "
//                   << g_writeOffset << std::endl;
//     } else {
//         std::cerr << "[A] Not enough space in shared memory! (Offset=" 
//                   << g_writeOffset << ")" << std::endl;
//     }
//     return Status::OK;
// }

// };

// void RunNodeA(const std::string& jsonFile, const std::string& nodeName) {

//     // LOG: Let’s show the args
//     std::cout << "[A] RunNodeA called with jsonFile=" << jsonFile
//               << ", nodeName=" << nodeName << std::endl;

//     NodeConfig config;
//     if (!loadNodeConfig(jsonFile, nodeName, config)) {
//         std::cerr << "[A] Failed to load config for nodeName=" << nodeName << std::endl;
//         return;
//     }

//     // LOG: show the config address
//     std::cout << "[A] According to config, listenAddress=" << config.listenAddress << std::endl;

//     // Setup shared memory
//     shm_unlink(SHM_NAME);
//     int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
//     if (fd < 0) {
//         std::cerr << "[A] shm_open failed (fd<0)!" << std::endl;
//         return;
//     }
//     if (ftruncate(fd, SHM_SIZE) != 0) {
//         std::cerr << "[A] ftruncate failed on shared memory." << std::endl;
//         close(fd);
//         return;
//     }
//     g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
//     close(fd);
//     if (g_shmPtr == MAP_FAILED) {
//         std::cerr << "[A] mmap failed! Can't attach shared memory." << std::endl;
//         return;
//     }
//     // LOG: confirm success
//     std::cout << "[A] Successfully attached shared memory at " << SHM_NAME << std::endl;

//     // Build the gRPC server
//     NodeAServiceImpl service;
//     grpc::ServerBuilder builder;

//     builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
//     builder.RegisterService(&service);

//     std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
//     if (!server) {
//         std::cerr << "[A] Failed to build/start gRPC server on " << config.listenAddress << std::endl;
//         return;
//     }

//     std::cout << "[A] Listening on " << config.listenAddress << std::endl;
//     // LOG: blocking call - waiting for RPC calls
//     server->Wait();

//     // Cleanup if server->Wait() exits
//     std::cout << "[A] Server shutting down. Cleaning up shared memory.\n";
//     munmap(g_shmPtr, SHM_SIZE);
//     shm_unlink(SHM_NAME);
// }

// int main(int argc, char** argv) {
//     if (argc < 3) {
//         std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
//         return 1;
//     }
//     std::string jsonFile = argv[1];
//     std::string nodeName = argv[2];

//     RunNodeA(jsonFile, nodeName);
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
static const size_t SHM_SIZE = 500 * 1024 * 1024;

// Global resources for shared memory
std::mutex g_mutex;
char* g_shmPtr = nullptr;
size_t g_writeOffset = 0;
size_t g_totalRowsWritten = 0;  // Counter for total rows written

class NodeAServiceImpl final : public DataService::Service {
public:
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        std::cout << "[A] Received row: " << request->row_data() << std::endl;

        std::lock_guard<std::mutex> lock(g_mutex);
        std::string row = request->row_data() + "\n";

        std::cout << "[A] Current g_writeOffset: " << g_writeOffset
                  << ", row size: " << row.size() << std::endl;

        if (g_writeOffset + row.size() < SHM_SIZE) {
            memcpy(g_shmPtr + g_writeOffset, row.data(), row.size());
            g_writeOffset += row.size();
            g_totalRowsWritten++;  // Increment the counter
            std::cout << "[A] Wrote row to shared memory. New g_writeOffset: "
                      << g_writeOffset << std::endl;
            std::cout << "[A] Total rows written so far: " << g_totalRowsWritten << std::endl;
        } else {
            std::cerr << "[A] Not enough space in shared memory! (Offset=" 
                      << g_writeOffset << ")" << std::endl;
        }
        return Status::OK;
    }
};

void RunNodeA(const std::string& jsonFile, const std::string& nodeName) {
    std::cout << "[A] RunNodeA called with jsonFile=" << jsonFile
              << ", nodeName=" << nodeName << std::endl;

    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[A] Failed to load config for nodeName=" << nodeName << std::endl;
        return;
    }

    std::cout << "[A] According to config, listenAddress=" << config.listenAddress << std::endl;

    // Setup shared memory
    shm_unlink(SHM_NAME);
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        std::cerr << "[A] shm_open failed (fd<0)!" << std::endl;
        return;
    }
    if (ftruncate(fd, SHM_SIZE) != 0) {
        std::cerr << "[A] ftruncate failed on shared memory." << std::endl;
        close(fd);
        return;
    }
    g_shmPtr = (char*)mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (g_shmPtr == MAP_FAILED) {
        std::cerr << "[A] mmap failed! Can't attach shared memory." << std::endl;
        return;
    }
    std::cout << "[A] Successfully attached shared memory at " << SHM_NAME << std::endl;

    // Build the gRPC server
    NodeAServiceImpl service;
    grpc::ServerBuilder builder;

    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "[A] Failed to build/start gRPC server on " << config.listenAddress << std::endl;
        return;
    }

    std::cout << "[A] Listening on " << config.listenAddress << std::endl;
    
    // Register cleanup function to log final count
    std::atexit([]() {
        std::cout << "[A] FINAL TOTAL ROWS WRITTEN TO SHM: " << g_totalRowsWritten << std::endl;
    });

    server->Wait();

    // Cleanup
    std::cout << "[A] Server shutting down. Cleaning up shared memory.\n";
    std::cout << "[A] Final row count: " << g_totalRowsWritten << " rows written to shared memory\n";
    std::cout << "[A] FINAL SHM USAGE: " << g_writeOffset << "/" << SHM_SIZE 
          << " bytes (" << (g_writeOffset*100.0/SHM_SIZE) << "% used)\n";
    munmap(g_shmPtr, SHM_SIZE);
    shm_unlink(SHM_NAME);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    std::string jsonFile = argv[1];
    std::string nodeName = argv[2];

    RunNodeA(jsonFile, nodeName);
    return 0;
}