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
#include <csignal> // <-- for std::signal and SIGINT

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
        g_receivedRows.push_back(request->row_data());
        std::cout << "[E] Received row: " << request->row_data() << "\n";
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

    g_server = builder.BuildAndStart();
    std::cout << "[E] Listening on " << config.listenAddress << "\n";

    // Will block until handleSigint calls Shutdown()
    g_server->Wait();

    // Final summary after normal shutdown
    std::cout << "\n[E] FINAL SUMMARY:\n";
    if (!g_receivedRows.empty()) {
        std::cout << "  - TOTAL RECEIVED: " << g_receivedRows.size() << " rows\n"
                  << "  - FIRST ROW: " << g_receivedRows.front() << "\n"
                  << "  - LAST ROW: " << g_receivedRows.back() << "\n";
    } else {
        std::cout << "  - NO ROWS RECEIVED!\n";
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <nodeName>\n";
        return 1;
    }
    RunNodeE(argv[1], argv[2]);
    return 0;
}
