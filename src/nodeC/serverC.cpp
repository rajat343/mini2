#include <iostream>
#include <vector>
#include <csignal>
#include <memory>
#include <chrono>
#include <numeric>

#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;
using dataflow::DataService;
using dataflow::Record;

// Global tracker
std::vector<std::string> g_receivedRows;

// Timing metrics
std::vector<double> g_processingTimes;
double g_totalProcessingTime = 0.0;
auto g_serverStartTime = std::chrono::high_resolution_clock::now();

// Make the gRPC server a static/global so signal handler can call Shutdown()
static std::unique_ptr<Server> g_server;

// Signal handler
void handleSigint(int /* signum */) {
    if (g_server) {
        std::cout << "[C] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown();
    }
}

class NodeCServiceImpl final : public DataService::Service {
public:
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
    
    return Status::OK;
}
};

void RunNodeC(const std::string& jsonFile, const std::string& nodeName) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[C] Failed to load config.\n";
        return;
    }

    auto config_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();
    std::cout << "[C] Config loaded in " << (config_duration/1000000.0) << " seconds" << std::endl;

    NodeCServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

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