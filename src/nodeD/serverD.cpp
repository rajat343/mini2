#include <iostream>
#include <vector>
#include <memory>
#include <csignal>  // <-- for std::signal and SIGINT
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <chrono>  // for timing metrics

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;
using google::protobuf::Empty;
using dataflow::DataService;
using dataflow::Record;

static const float KEEP_PERCENT = 0.5f;

// Trackers
std::vector<std::string> g_keptRows;
std::vector<std::string> g_sentToERows;

// Global server pointer for graceful shutdown
static std::unique_ptr<Server> g_server;

// Performance metrics
auto g_startTime = std::chrono::high_resolution_clock::now();
double g_totalProcessingTime = 0.0;
int g_totalRecordsProcessed = 0;

// Signal handler
void handleSigint(int /* signum */) {
    if (g_server) {
        std::cout << "[D] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown();
    }
}

class NodeDServiceImpl final : public DataService::Service {
    std::unique_ptr<DataService::Stub> stubE;
public:
Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
    // Start timing for this record
    auto startTime = std::chrono::high_resolution_clock::now();
    
    // Decide whether to keep or send to E
    if (rand() % 100 < (KEEP_PERCENT * 100)) {
        g_keptRows.push_back(request->row_data());
        // Only log occasionally
        if (g_keptRows.size() % 100 == 0) {
            std::cout << "[D] Kept row #" << g_keptRows.size() << "\n";
        }
    } else {
        if (!stubE) {
            // Hard-coded address here for demonstration; in real code, read from config
            stubE = DataService::NewStub(
                grpc::CreateChannel("localhost:50055", grpc::InsecureChannelCredentials())
            );
        }
        g_sentToERows.push_back(request->row_data());
        Record record;
        record.set_row_data(request->row_data());
        Empty e;
        ClientContext ctx;
        stubE->SendRecord(&ctx, record, &e);
        
        // Only log occasionally
        if (g_sentToERows.size() % 100 == 0) {
            std::cout << "[D] Sent to E: row #" << g_sentToERows.size() << "\n";
        }
    }
    
    // End timing for this record
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - startTime).count();
    double durationSec = duration / 1000000.0;
    
    // Update metrics
    g_totalProcessingTime += durationSec;
    g_totalRecordsProcessed++;
    
    // Log processing time only periodically
    if (g_totalRecordsProcessed % 100 == 0) {
        std::cout << "[D] Processed record #" << g_totalRecordsProcessed 
                << " in " << durationSec << " seconds\n";
    
        // Print periodic statistics
        auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::high_resolution_clock::now() - g_startTime).count();
        
        std::cout << "[D] PERFORMANCE METRICS:\n"
                << "  - Records processed: " << g_totalRecordsProcessed << "\n"
                << "  - Average processing time: " << (g_totalProcessingTime / g_totalRecordsProcessed) << " sec/record\n"
                << "  - Throughput: " << (g_totalRecordsProcessed / (totalTime > 0 ? totalTime : 1)) << " records/sec\n";
    }
    
    return Status::OK;
}
};

void RunNodeD(const std::string& jsonFile, const std::string& nodeName) {
    NodeConfig config;
    if (!loadNodeConfig(jsonFile, nodeName, config)) {
        std::cerr << "[D] Failed to load config.\n";
        return;
    }

    NodeDServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Install our signal handler
    std::signal(SIGINT, handleSigint);

    // Reset start time before server starts
    g_startTime = std::chrono::high_resolution_clock::now();
    
    g_server = builder.BuildAndStart();
    std::cout << "[D] Listening on " << config.listenAddress << "\n";

    // Will block here until handleSigint calls Shutdown()
    g_server->Wait();

    // Calculate final performance metrics
    auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - g_startTime).count();

    // Final summary after normal shutdown
    std::cout << "\n[D] FINAL SUMMARY:\n";
    if (!g_keptRows.empty()) {
        std::cout << "  - KEPT: " << g_keptRows.size() << " rows\n"
                  << "    FIRST: " << g_keptRows.front() << "\n"
                  << "    LAST: " << g_keptRows.back() << "\n";
    }
    if (!g_sentToERows.empty()) {
        std::cout << "  - SENT TO E: " << g_sentToERows.size() << " rows\n"
                  << "    FIRST: " << g_sentToERows.front() << "\n"
                  << "    LAST: " << g_sentToERows.back() << "\n";
    }
    
    // Print final performance metrics
    std::cout << "\n[D] FINAL PERFORMANCE METRICS:\n"
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
    RunNodeD(argv[1], argv[2]);
    return 0;
}