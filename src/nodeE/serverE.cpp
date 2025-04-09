#include <iostream>
#include <vector>
#include <memory>
#include <csignal> // <-- for std::signal and SIGINT
#include <chrono>  // for timing metrics

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

// Global server pointer for graceful shutdown
static std::unique_ptr<Server> g_server;

// Performance metrics
auto g_startTime = std::chrono::high_resolution_clock::now();
double g_totalProcessingTime = 0.0;
int g_totalRecordsProcessed = 0;

// Signal handler
void handleSigint(int /* signum */) {
    if (g_server) {
        std::cout << "[E] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown();
    }
}

class NodeEServiceImpl final : public DataService::Service {
public:
Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
    static const int EXPECTED_RECORDS = 537914;  // Update based on client record count

    if (request->row_data() == "__START__") {
        g_startTime = std::chrono::high_resolution_clock::now();
        std::cout << "[E] ⏱ Received __START__ signal. Timer started.\n";
        return Status::OK;
    }

    // Start timing for this record
    auto startTime = std::chrono::high_resolution_clock::now();
    
    g_receivedRows.push_back(request->row_data());
    
    // Only log occasionally
    if (g_receivedRows.size() % 100 == 0) {
        std::cout << "[E] Received row #" << g_receivedRows.size() << "\n";
    }
    
    // End timing for this record
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - startTime).count();
    double durationSec = duration / 1000000.0;
    
    // Update metrics
    g_totalProcessingTime += durationSec;
    g_totalRecordsProcessed++;

    if (g_totalRecordsProcessed == EXPECTED_RECORDS) {
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - g_startTime).count();
        std::cout << "[E] ✅ FINAL RECORD RECEIVED.\n";
        std::cout << "[E] 🕒 END-TO-END TOTAL TIME (from client start): " << duration << " seconds\n";
    }

    
    // Log processing time only periodically
    if (g_totalRecordsProcessed % 100 == 0) {
        std::cout << "[E] Processed record #" << g_totalRecordsProcessed 
                << " in " << durationSec << " seconds\n";
    
        // Print periodic statistics
        auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::high_resolution_clock::now() - g_startTime).count();
        
        std::cout << "[E] PERFORMANCE METRICS:\n"
                << "  - Records processed: " << g_totalRecordsProcessed << "\n"
                << "  - Average processing time: " << (g_totalProcessingTime / g_totalRecordsProcessed) << " sec/record\n"
                << "  - Throughput: " << (g_totalRecordsProcessed / (totalTime > 0 ? totalTime : 1)) << " records/sec\n";
    }
    
    return Status::OK;
}
};

void RunNodeE(const std::string& jsonFile, const std::string& nodeName) {
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[E] Failed to load config.\n";
        return;
    }

    NodeEServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Install our signal handler
    std::signal(SIGINT, handleSigint);

    // Reset start time before server starts
    g_startTime = std::chrono::high_resolution_clock::now();
    
    g_server = builder.BuildAndStart();
    std::cout << "[E] Listening on " << config.listenAddress << "\n";

    // Will block until handleSigint calls Shutdown()
    g_server->Wait();

    // Calculate final performance metrics
    auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - g_startTime).count();

    // Final summary after normal shutdown
    std::cout << "\n[E] FINAL SUMMARY:\n";
    if (!g_receivedRows.empty()) {
        std::cout << "  - TOTAL RECEIVED: " << g_receivedRows.size() << " rows\n"
                  << "  - FIRST ROW: " << g_receivedRows.front() << "\n"
                  << "  - LAST ROW: " << g_receivedRows.back() << "\n";
    } else {
        std::cout << "  - NO ROWS RECEIVED!\n";
    }
    
    // Print final performance metrics
    std::cout << "\n[E] FINAL PERFORMANCE METRICS:\n"
              << "  - Total runtime: " << totalTime << " seconds\n"
              << "  - Total records processed: " << g_totalRecordsProcessed << "\n"
              << "  - Average processing time: " << (g_totalRecordsProcessed > 0 ? g_totalProcessingTime / g_totalRecordsProcessed : 0) << " sec/record\n"
              << "  - Overall throughput: " << (totalTime > 0 ? g_totalRecordsProcessed / totalTime : 0) << " records/sec\n";
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeE(argv[1], argv[2]);
    return 0;
}