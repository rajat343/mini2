#include <iostream>
#include <vector>
#include <csignal>
#include <memory>
#include <chrono>
#include <numeric>
#include <iomanip>

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
using dataflow::RecordBatch;

// Global tracker
std::vector<std::string> g_receivedRows;

// Timing metrics
std::vector<double> g_processingTimes;
double g_totalProcessingTime = 0.0;
auto g_serverStartTime = std::chrono::high_resolution_clock::now();

// Batch processing metrics
size_t g_batchCount = 0;
size_t g_totalRowsInBatches = 0;
std::vector<double> g_batchProcessingTimes;
double g_totalBatchProcessingTime = 0.0;

// Make the gRPC server a static/global so signal handler can call Shutdown()
static std::unique_ptr<Server> g_server;

// Channel to Node E for forwarding
std::unique_ptr<DataService::Stub> g_nodeEStub;

// Signal handler
void handleSigint(int /* signum */) {
    if (g_server) {
        std::cout << "[C] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown();
    }
}

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
    
    double elapsed() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        return duration / 1000000.0;
    }
};

class NodeCServiceImpl final : public DataService::Service {
public:
    // Keep the original single record method for backward compatibility
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        g_receivedRows.push_back(request->row_data());
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();
            
        double seconds = duration / 1000000.0;
        g_processingTimes.push_back(seconds);
        g_totalProcessingTime += seconds;
            
        // Only log every 100 records
        if (g_receivedRows.size() % 100 == 0) {
            std::cout << "[C] Received row #" << g_receivedRows.size() 
                    << " in " << seconds << " seconds" << std::endl;
        
            // Calculate average processing time
            std::cout << "[C] Average processing time per record: " 
                    << (g_totalProcessingTime / g_receivedRows.size()) << " seconds" << std::endl;
                    
            // Calculate throughput
            auto total_runtime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - g_serverStartTime).count();
                
            double records_per_second = g_receivedRows.size() / (double)total_runtime;
            std::cout << "[C] Current throughput: " << records_per_second << " records/second" << std::endl;
        }
        
        // Forward single record to Node E if we have a connection
        if (g_nodeEStub) {
            ClientContext forward_ctx;
            Empty forward_response;
            g_nodeEStub->SendRecord(&forward_ctx, *request, &forward_response);
        }
        
        return Status::OK;
    }
    
    // New batch processing method
    Status SendRecordBatch(ServerContext* context, const RecordBatch* request, Empty* response) override {
        Timer timer("C::SendRecordBatch");
        
        int batch_size = request->records_size();
        g_batchCount++;
        g_totalRowsInBatches += batch_size;
        
        // Process the batch
        for (int i = 0; i < batch_size; i++) {
            g_receivedRows.push_back(request->records(i).row_data());
        }
        
        // Track processing time
        double batch_seconds = timer.elapsed();
        g_batchProcessingTimes.push_back(batch_seconds);
        g_totalBatchProcessingTime += batch_seconds;
        
        // Log only occasionally to avoid spamming the console
        bool should_log = (g_batchCount % 10 == 0);
        
        if (should_log) {
            std::cout << "[C] Received batch #" << g_batchCount 
                     << " with " << batch_size << " records in " 
                     << batch_seconds << " seconds" << std::endl;
            
            std::cout << "[C] Total rows via batches: " << g_totalRowsInBatches << std::endl;
            std::cout << "[C] Average batch size: " << (g_totalRowsInBatches / g_batchCount) << std::endl;
            std::cout << "[C] Average batch processing time: " 
                     << (g_totalBatchProcessingTime / g_batchCount) << " seconds" << std::endl;
            
            // Calculate throughput
            auto total_runtime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - g_serverStartTime).count();
                
            double records_per_second = g_receivedRows.size() / (double)total_runtime;
            std::cout << "[C] Current throughput: " << records_per_second << " records/second" << std::endl;
        }
        
        // Forward the batch to Node E if we have a connection
        if (g_nodeEStub) {
            ClientContext forward_ctx;
            Empty forward_response;
            
            // Set timeout for large batches
            forward_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
            
            Status status = g_nodeEStub->SendRecordBatch(&forward_ctx, *request, &forward_response);
            
            if (!status.ok() && should_log) {
                std::cerr << "[C] Error forwarding batch to Node E: " 
                         << status.error_message() << std::endl;
            }
        }
        
        return Status::OK;
    }
};

void RunNodeC(const std::string& jsonFile, const std::string& nodeName) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::cout << "[C] Starting with batch processing enabled" << std::endl;
    
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[C] Failed to load config.\n";
        return;
    }

    auto config_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();
    std::cout << "[C] Config loaded in " << (config_duration/1000000.0) << " seconds" << std::endl;

    // Set up connection to Node E if it's in the neighbors list
    if (config.neighbors.count("E") > 0) {
        std::string nodeEAddress = config.neighbors["E"];
        
        grpc::ChannelArguments args;
        args.SetMaxSendMessageSize(50 * 1024 * 1024); // 50MB
        args.SetMaxReceiveMessageSize(50 * 1024 * 1024); // 50MB
        
        auto channel = grpc::CreateCustomChannel(
            nodeEAddress, 
            grpc::InsecureChannelCredentials(),
            args
        );
        
        g_nodeEStub = DataService::NewStub(channel);
        std::cout << "[C] Set up connection to Node E at " << nodeEAddress << std::endl;
    } else {
        std::cout << "[C] No Node E in neighbors list, will not forward data" << std::endl;
    }

    NodeCServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    
    // Set max message size for batches
    builder.SetMaxReceiveMessageSize(50 * 1024 * 1024); // 50MB
    builder.SetMaxSendMessageSize(50 * 1024 * 1024); // 50MB

    // Install our signal handler
    std::signal(SIGINT, handleSigint);

    auto server_build_start = std::chrono::high_resolution_clock::now();
    g_server = builder.BuildAndStart();
    
    auto server_build_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - server_build_start).count();
    std::cout << "[C] Server built in " << (server_build_duration/1000000.0) << " seconds" << std::endl;
    
    std::cout << "[C] Listening on " << config.listenAddress << "\n";
    
    // Reset server start time just before Wait()
    g_serverStartTime = std::chrono::high_resolution_clock::now();

    // Will block until handleSigint calls Shutdown()
    g_server->Wait();

    // Calculate final stats
    auto total_runtime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - g_serverStartTime).count();
    
    // Final summary after normal shutdown
    std::cout << "\n[C] FINAL SUMMARY:\n";
    if (!g_receivedRows.empty()) {
        std::cout << "  - TOTAL RECEIVED: " << g_receivedRows.size() << " rows\n"
                  << "  - FIRST ROW: " << g_receivedRows.front() << "\n"
                  << "  - LAST ROW: " << g_receivedRows.back() << "\n";
    } else {
        std::cout << "  - NO ROWS RECEIVED!\n";
    }
    
    // Print timing statistics
    std::cout << "\n[C] TIMING STATISTICS:\n";
    std::cout << "  - Server ran for: " << total_runtime << " seconds\n";
    std::cout << "  - Total records processed: " << g_receivedRows.size() << "\n";
    std::cout << "  - Average throughput: " 
              << (total_runtime > 0 ? g_receivedRows.size() / (double)total_runtime : 0) 
              << " records/second\n";
              
    if (!g_processingTimes.empty()) {
        double min_time = *std::min_element(g_processingTimes.begin(), g_processingTimes.end());
        double max_time = *std::max_element(g_processingTimes.begin(), g_processingTimes.end());
        double avg_time = g_totalProcessingTime / g_processingTimes.size();
        
        std::cout << "  - Min processing time: " << min_time << " seconds\n";
        std::cout << "  - Max processing time: " << max_time << " seconds\n";
        std::cout << "  - Avg processing time: " << avg_time << " seconds\n";
    }
}

int main(int argc, char** argv) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeC(argv[1], argv[2]);
    
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();
        
    std::cout << "[C] Total execution time: " << total_duration << " seconds" << std::endl;
    
    return 0;
}