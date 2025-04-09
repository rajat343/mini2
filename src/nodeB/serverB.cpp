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
#include <csignal>

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

// Timing metrics
double g_totalReadTime = 0.0;
double g_totalDistributeTime = 0.0;
size_t g_totalIterations = 0;
size_t g_totalLinesProcessed = 0;

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
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // We don't do anything in B's SendRecord, since data is coming from A->B via shared memory
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();
            
        std::cout << "[B] Processed SendRecord request in " << (duration/1000000.0) << " seconds" << std::endl;
        
        return Status::OK;
    }
};

void forwardTo(const std::string& neighborName, const std::string& data) {
    static std::map<std::string, int> forwardCounters;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
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
    
    // Increment counter and log only every 100 forwards
    forwardCounters[neighborName]++;
    if (forwardCounters[neighborName] % 100 == 0) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();
            
        std::cout << "[B] Forwarded record #" << forwardCounters[neighborName] 
                 << " to " << neighborName << " in " << (duration/1000000.0) << " seconds" << std::endl;
    }
}

void readAndDistribute() {
    auto thread_start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::string> lines;
    size_t lastOffset = 0;
    bool isFirstLine = true;
    int iteration_counter = 0;
    
    while (true) {
        iteration_counter++;
        bool should_log = (iteration_counter % 10 == 0); // Log every 10 iterations
        
        auto iteration_start = std::chrono::high_resolution_clock::now();
        size_t linesReadThisIteration = 0;
        
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            auto read_start = std::chrono::high_resolution_clock::now();
            
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
                    linesReadThisIteration++;
                }
                else if (g_shmPtr[g_readOffset] == '\0') {
                    // no more data in the shared memory
                    break;
                }
                else {
                    g_readOffset++;
                }
            }
            
            auto read_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - read_start).count();
                
            if (linesReadThisIteration > 0 && should_log) {
                g_totalReadTime += read_duration / 1000000.0;
                std::cout << "[B] Read " << linesReadThisIteration << " lines in " 
                          << (read_duration/1000000.0) << " seconds" << std::endl;
            }
        }

        if (!lines.empty()) {
            auto distribute_start = std::chrono::high_resolution_clock::now();
            
            size_t total = lines.size();
            size_t keepCnt = static_cast<size_t>(total * KEEP_PERCENT);
            size_t cCnt = static_cast<size_t>(total * SEND_TO_C_PERCENT);
            size_t dCnt = total - keepCnt - cCnt;

            // Keep rows - remove individual row logging
            for (size_t i = 0; i < keepCnt; i++) {
                g_keptRows.push_back(lines[i]);
            }
            
            if (should_log) {
                std::cout << "[B] Added " << keepCnt << " rows to g_keptRows (total now " 
                          << g_keptRows.size() << ")\n";
            }

            // Send to C - remove individual row logging
            for (size_t i = keepCnt; i < keepCnt + cCnt; i++) {
                g_sentToCRows.push_back(lines[i]);
                forwardTo("C", lines[i]);
            }
            
            if (should_log) {
                std::cout << "[B] Added " << cCnt << " rows to g_sentToCRows (total now " 
                          << g_sentToCRows.size() << ")\n";
            }

            // Send to D - remove individual row logging
            for (size_t i = keepCnt + cCnt; i < total; i++) {
                g_sentToDRows.push_back(lines[i]);
                forwardTo("D", lines[i]);
            }
            
            if (should_log) {
                std::cout << "[B] Added " << dCnt << " rows to g_sentToDRows (total now " 
                          << g_sentToDRows.size() << ")\n";
            }
            
            auto distribute_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - distribute_start).count();
                
            g_totalDistributeTime += distribute_duration / 1000000.0;
            
            if (should_log) {
                std::cout << "[B] Distributed " << lines.size() << " lines in " 
                         << (distribute_duration/1000000.0) << " seconds" << std::endl;
            }
                     
            g_totalLinesProcessed += lines.size();
            lines.clear();
        }
        
        auto iteration_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - iteration_start).count();
            
        if (linesReadThisIteration > 0 && should_log) {
            g_totalIterations++;
            std::cout << "[B] Total iteration time: " << (iteration_duration/1000000.0) 
                     << " seconds" << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void RunNodeB(const std::string& jsonFile, const std::string& nodeName) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
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

    auto setup_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();
    std::cout << "[B] Initial setup completed in " << (setup_duration/1000000.0) << " seconds" << std::endl;

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
    
    // Print timing statistics
    std::cout << "\n[B] TIMING STATISTICS:\n";
    std::cout << "  - Total iterations: " << g_totalIterations << "\n";
    std::cout << "  - Total lines processed: " << g_totalLinesProcessed << "\n";
    std::cout << "  - Total read time: " << g_totalReadTime << " seconds\n";
    std::cout << "  - Total distribute time: " << g_totalDistributeTime << " seconds\n";
    std::cout << "  - Average read time per iteration: " << (g_totalIterations > 0 ? g_totalReadTime / g_totalIterations : 0) << " seconds\n";
    std::cout << "  - Average distribute time per iteration: " << (g_totalIterations > 0 ? g_totalDistributeTime / g_totalIterations : 0) << " seconds\n";
    std::cout << "  - Average time per line: " << (g_totalLinesProcessed > 0 ? (g_totalReadTime + g_totalDistributeTime) / g_totalLinesProcessed : 0) << " seconds\n";

    // cleanup shared memory if you like
    munmap(g_shmPtr, SHM_SIZE);
    // shm_unlink(SHM_NAME); // optional if you want to remove it
}

int main(int argc, char** argv) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeB(argv[1], argv[2]);
    
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();
        
    std::cout << "[B] Total execution time: " << total_duration << " seconds" << std::endl;
    
    return 0;
}