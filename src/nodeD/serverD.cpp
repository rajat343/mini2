// #include <iostream>
// #include <grpcpp/grpcpp.h>
// #include "data.pb.h"
// #include "data.grpc.pb.h"
// #include "read_config.h"

// using grpc::Server;
// using grpc::ServerBuilder;
// using grpc::ServerContext;
// using grpc::Status;
// using google::protobuf::Empty;
// using dataflow::DataService;
// using dataflow::Record;

// class NodeCServiceImpl final : public DataService::Service {
// public:
//     Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
//         std::cout << "[C] Received: " << request->row_data() << std::endl;
//         return Status::OK;
//     }
// };

// void RunNodeC(const std::string& jsonFile, const std::string& nodeName) {
//     NodeConfig config;
//     if (!loadNodeConfig(jsonFile, nodeName, config)) {
//         std::cerr << "Failed to load config for node C\n";
//         return;
//     }

//     NodeCServiceImpl service;
//     ServerBuilder builder;
//     builder.AddListeningPort(config.listenAddress, grpc::InsecureServerCredentials());
//     builder.RegisterService(&service);

//     auto server = builder.BuildAndStart();
//     std::cout << "[C] Listening on " << config.listenAddress << std::endl;
//     server->Wait();
// }

// int main(int argc, char** argv) {
//     // usage: serverC <config.json> C
//     if (argc < 3) {
//         std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
//         return 1;
//     }
//     RunNodeC(argv[1], argv[2]);
//     return 0;
// }


#include <iostream>
#include <vector>
#include <memory>
#include <csignal>  // <-- for std::signal and SIGINT
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

static const float KEEP_PERCENT = 0.5f;

// Trackers
std::vector<std::string> g_keptRows;
std::vector<std::string> g_sentToERows;

// Global server pointer for graceful shutdown
static std::unique_ptr<Server> g_server;

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
        // Decide whether to keep or send to E
        if (rand() % 100 < (KEEP_PERCENT * 100)) {
            g_keptRows.push_back(request->row_data());
            std::cout << "[D] Kept row: " << request->row_data() << "\n";
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
            std::cout << "[D] Sent to E: " << request->row_data() << "\n";
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

    g_server = builder.BuildAndStart();
    std::cout << "[D] Listening on " << config.listenAddress << "\n";

    // Will block here until handleSigint calls Shutdown()
    g_server->Wait();

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
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeD(argv[1], argv[2]);
    return 0;
}
