#include <iostream>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <chrono>
#include <iomanip>

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
using dataflow::RecordBatch;

static const char* SHM_NAME = "/shm_A_to_B";
static const size_t SHM_SIZE = 500 * 1024 * 1024;

// Global resources for shared memory
std::mutex g_mutex;
char* g_shmPtr = nullptr;
size_t g_writeOffset = 0;
size_t g_totalRowsWritten = 0;  // Counter for total rows written
size_t g_totalBatchesReceived = 0; // Counter for batches

class Timer {
    private:
        std::chrono::high_resolution_clock::time_point start_time;
        std::string name;
    public:
        Timer(const std::string& timer_name) : name(timer_name) {
            start_time = std::chrono::high_resolution_clock::now();
        }
        
        ~Timer() {
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
            std::cout << "[TIMER] " << name << ": " << std::fixed << std::setprecision(6) 
                      << (duration / 1000000.0) << " seconds" << std::endl;
        }
    };

class NodeAServiceImpl final : public DataService::Service {
public:
    // Original single record method (kept for backward compatibility)
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        Timer timer("A::SendRecord(single)");
        
        // Only log every 100 records
        static int log_counter = 0;
        bool should_log = (++log_counter % 1000 == 0);
        
        if (should_log) {
            std::cout << "[A] Received single row " << log_counter << ": " << request->row_data() << std::endl;
        }

        std::lock_guard<std::mutex> lock(g_mutex);
        std::string row = request->row_data() + "\n";

        if (g_writeOffset + row.size() < SHM_SIZE) {
            auto write_start = std::chrono::high_resolution_clock::now();
            memcpy(g_shmPtr + g_writeOffset, row.data(), row.size());
            auto write_end = std::chrono::high_resolution_clock::now();
            auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count();
            
            g_writeOffset += row.size();
            g_totalRowsWritten++;  // Increment the counter
            
            if (should_log) {
                std::cout << "[A] Wrote row to shared memory in " << (write_duration / 1000000.0) 
                        << " seconds. New g_writeOffset: " << g_writeOffset << std::endl;
                std::cout << "[A] Total rows written so far: " << g_totalRowsWritten << std::endl;
            }
        } else {
            std::cerr << "[A] Not enough space in shared memory! (Offset=" 
                    << g_writeOffset << ")" << std::endl;
        }
        return Status::OK;
    }

    // New batch processing method
    Status SendRecordBatch(ServerContext* context, const RecordBatch* request, Empty* response) override {
        Timer timer("A::SendRecordBatch");
        
        // Get batch size
        int batch_size = request->records_size();
        g_totalBatchesReceived++;



        
        // Log only occasionally
        bool should_log = (g_totalBatchesReceived % 10 == 0);
        
        if (should_log) {
            std::cout << "[A] Received batch #" << g_totalBatchesReceived 
                     << " with " << batch_size << " records" << std::endl;
        }
        
        // Prepare a single large buffer to minimize lock contention and memory operations
        std::string combined_data;
        combined_data.reserve(batch_size * 100);  // Rough estimate of size needed
        
        for (int i = 0; i < batch_size; i++) {
            combined_data += request->records(i).row_data() + "\n";
        }
        
        // Now write the whole batch at once under one lock
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            
            if (g_writeOffset + combined_data.size() < SHM_SIZE) {
                auto write_start = std::chrono::high_resolution_clock::now();
                memcpy(g_shmPtr + g_writeOffset, combined_data.data(), combined_data.size());
                auto write_end = std::chrono::high_resolution_clock::now();
                auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count();
                
                g_writeOffset += combined_data.size();
                g_totalRowsWritten += batch_size;  // Increment the counter
                
                if (should_log) {
                    std::cout << "[A] Wrote batch of " << batch_size << " rows to shared memory in " 
                             << (write_duration / 1000000.0) << " seconds" << std::endl;
                    std::cout << "[A] New g_writeOffset: " << g_writeOffset << std::endl;
                    std::cout << "[A] Total rows written so far: " << g_totalRowsWritten << std::endl;
                    std::cout << "[A] Total batches received: " << g_totalBatchesReceived << std::endl;
                    std::cout << "[A] Average batch size: " << (float)g_totalRowsWritten / g_totalBatchesReceived << std::endl;
                }
            } else {
                std::cerr << "[A] Not enough space in shared memory for batch! (Offset=" 
                        << g_writeOffset << ", batch size=" << combined_data.size() << ")" << std::endl;
            }
        }
        
        return Status::OK;
    }
};

std::chrono::high_resolution_clock::time_point g_serverStartTime;
std::chrono::high_resolution_clock::time_point g_firstRecordTime;
bool g_firstRecordProcessed = false;

void RunNodeA(const std::string& jsonFile, const std::string& nodeName) {

    g_serverStartTime = std::chrono::high_resolution_clock::now();
    
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

    // Increase max message size for large batches (default is 4MB)
    builder.SetMaxReceiveMessageSize(50 * 1024 * 1024); // 50MB
    
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "[A] Failed to build/start gRPC server on " << config.listenAddress << std::endl;
        return;
    }

    std::cout << "[A] Listening on " << config.listenAddress << std::endl;

    auto serverRunDuration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - g_serverStartTime).count();
    std::cout << "[A] Server ran for " << serverRunDuration << " seconds." << std::endl;
    
    if (g_firstRecordProcessed) {
        auto processingDuration = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::high_resolution_clock::now() - g_firstRecordTime).count();
        std::cout << "[A] Processing time (first record to shutdown): " 
                  << processingDuration << " seconds." << std::endl;
    }
    
    // Register cleanup function to log final count
    std::atexit([]() {
        std::cout << "[A] FINAL TOTAL ROWS WRITTEN TO SHM: " << g_totalRowsWritten << std::endl;
        std::cout << "[A] TOTAL BATCHES RECEIVED: " << g_totalBatchesReceived << std::endl;
        if (g_totalBatchesReceived > 0) {
            std::cout << "[A] AVERAGE BATCH SIZE: " << (float)g_totalRowsWritten / g_totalBatchesReceived << std::endl;
        }
    });

    server->Wait();

    // Cleanup
    std::cout << "[A] Server shutting down. Cleaning up shared memory.\n";
    std::cout << "[A] Final row count: " << g_totalRowsWritten << " rows written to shared memory\n";
    std::cout << "[A] Final batch count: " << g_totalBatchesReceived << " batches received\n";
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