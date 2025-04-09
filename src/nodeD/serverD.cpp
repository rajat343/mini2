#include <iostream>
#include <vector>
#include <memory>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include "data.pb.h"
#include "data.grpc.pb.h"
#include "read_config.h"
#include <chrono>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;
using google::protobuf::Empty;
using dataflow::DataService;
using dataflow::Record;
using dataflow::RecordBatch;

static const float KEEP_PERCENT = 0.5f;
static const int BATCH_SIZE = 5119;

std::vector<std::string> g_keptRows;
std::vector<std::string> g_sentToERows;
std::vector<std::string> g_batchBuffer;

static std::unique_ptr<Server> g_server;
auto g_startTime = std::chrono::high_resolution_clock::now();
double g_totalProcessingTime = 0.0;
int g_totalRecordsProcessed = 0;

void handleSigint(int) {
    if (g_server) {
        std::cout << "[D] Received SIGINT, shutting down server gracefully...\n";
        g_server->Shutdown();
    }
}

class NodeDServiceImpl final : public DataService::Service {
    std::unique_ptr<DataService::Stub> stubE;

    void ensureStubE() {
        if (!stubE) {
            stubE = DataService::NewStub(grpc::CreateChannel("localhost:50055", grpc::InsecureChannelCredentials()));
            std::cout << "[D] Initialized stubE for localhost:50055\n";
        }
    }

    void sendBatchToE() {
        if (g_batchBuffer.empty()) return;
        ensureStubE();

        RecordBatch batch;
        for (const auto& row : g_batchBuffer) {
            Record* rec = batch.add_records();
            rec->set_row_data(row);
            g_sentToERows.push_back(row);
        }

        Empty e;
        ClientContext ctx;
        Status status = stubE->SendRecordBatch(&ctx, batch, &e);

        if (!status.ok()) {
            std::cerr << "[D] \u274c Failed to send batch to Node E: "
                      << status.error_message() << " (code: " << status.error_code() << ")\n";
        } else {
            std::cout << "[D] \u2705 Sent batch of " << g_batchBuffer.size()
                      << " rows to E (Total sent: " << g_sentToERows.size() << ")\n";
        }

        g_batchBuffer.clear();
    }

public:
    Status SendRecord(ServerContext* context, const Record* request, Empty* response) override {
        if (request->row_data() == "__START__") {
            std::cout << "[D] \u23f1 Received __START__ signal. Forwarding to E...\n";
            ensureStubE();
            Record startRecord;
            startRecord.set_row_data("__START__");
            Empty e;
            ClientContext ctx;
            stubE->SendRecord(&ctx, startRecord, &e);
            return Status::OK;
        }

        g_totalRecordsProcessed++;

        if (rand() % 100 < (KEEP_PERCENT * 100)) {
            g_keptRows.push_back(request->row_data());
        } else {
            g_batchBuffer.push_back(request->row_data());
            if (g_batchBuffer.size() >= BATCH_SIZE) {
                sendBatchToE();
            }
        }

        return Status::OK;
    }

    Status SendRecordBatch(ServerContext* context, const RecordBatch* request, Empty* response) override {
        std::cout << "[D] Received batch of " << request->records_size() << " records from B\n";

        for (int i = 0; i < request->records_size(); ++i) {
            g_totalRecordsProcessed++;
            if (rand() % 100 < (KEEP_PERCENT * 100)) {
                g_keptRows.push_back(request->records(i).row_data());
            } else {
                g_batchBuffer.push_back(request->records(i).row_data());
            }

            if (g_batchBuffer.size() >= BATCH_SIZE) {
                sendBatchToE();
            }
        }

        return Status::OK;
    }

    Status EndStream(ServerContext* context, const Empty* request, Empty* response) override {
        std::cout << "[D] EndStream received. Sending remaining " << g_batchBuffer.size() << " rows...\n";
        sendBatchToE();

        if (stubE) {
            Empty req, resp;
            ClientContext ctx;
            stubE->EndStream(&ctx, req, &resp);
            std::cout << "[D] Forwarded EndStream to E\n";
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

    std::signal(SIGINT, handleSigint);
    g_startTime = std::chrono::high_resolution_clock::now();

    g_server = builder.BuildAndStart();
    std::cout << "[D] Listening on " << config.listenAddress << "\n";
    std::cout << "[D] Batch processing enabled with batch size: " << BATCH_SIZE << "\n";

    g_server->Wait();

    auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - g_startTime).count();

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