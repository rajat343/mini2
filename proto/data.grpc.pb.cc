// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: data.proto

#include "data.pb.h"
#include "data.grpc.pb.h"

#include <functional>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/message_allocator.h>
#include <grpcpp/support/method_handler.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/impl/server_callback_handlers.h>
#include <grpcpp/server_context.h>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/support/sync_stream.h>
namespace dataflow {

static const char* DataService_method_names[] = {
  "/dataflow.DataService/SendRecord",
  "/dataflow.DataService/SendRecordBatch",
  "/dataflow.DataService/EndStream",
};

std::unique_ptr< DataService::Stub> DataService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< DataService::Stub> stub(new DataService::Stub(channel, options));
  return stub;
}

DataService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_SendRecord_(DataService_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_SendRecordBatch_(DataService_method_names[1], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_EndStream_(DataService_method_names[2], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status DataService::Stub::SendRecord(::grpc::ClientContext* context, const ::dataflow::Record& request, ::google::protobuf::Empty* response) {
  return ::grpc::internal::BlockingUnaryCall< ::dataflow::Record, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_SendRecord_, context, request, response);
}

void DataService::Stub::async::SendRecord(::grpc::ClientContext* context, const ::dataflow::Record* request, ::google::protobuf::Empty* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::dataflow::Record, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendRecord_, context, request, response, std::move(f));
}

void DataService::Stub::async::SendRecord(::grpc::ClientContext* context, const ::dataflow::Record* request, ::google::protobuf::Empty* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendRecord_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::PrepareAsyncSendRecordRaw(::grpc::ClientContext* context, const ::dataflow::Record& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::google::protobuf::Empty, ::dataflow::Record, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_SendRecord_, context, request);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::AsyncSendRecordRaw(::grpc::ClientContext* context, const ::dataflow::Record& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncSendRecordRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status DataService::Stub::SendRecordBatch(::grpc::ClientContext* context, const ::dataflow::RecordBatch& request, ::google::protobuf::Empty* response) {
  return ::grpc::internal::BlockingUnaryCall< ::dataflow::RecordBatch, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_SendRecordBatch_, context, request, response);
}

void DataService::Stub::async::SendRecordBatch(::grpc::ClientContext* context, const ::dataflow::RecordBatch* request, ::google::protobuf::Empty* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::dataflow::RecordBatch, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendRecordBatch_, context, request, response, std::move(f));
}

void DataService::Stub::async::SendRecordBatch(::grpc::ClientContext* context, const ::dataflow::RecordBatch* request, ::google::protobuf::Empty* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendRecordBatch_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::PrepareAsyncSendRecordBatchRaw(::grpc::ClientContext* context, const ::dataflow::RecordBatch& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::google::protobuf::Empty, ::dataflow::RecordBatch, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_SendRecordBatch_, context, request);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::AsyncSendRecordBatchRaw(::grpc::ClientContext* context, const ::dataflow::RecordBatch& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncSendRecordBatchRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status DataService::Stub::EndStream(::grpc::ClientContext* context, const ::google::protobuf::Empty& request, ::google::protobuf::Empty* response) {
  return ::grpc::internal::BlockingUnaryCall< ::google::protobuf::Empty, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_EndStream_, context, request, response);
}

void DataService::Stub::async::EndStream(::grpc::ClientContext* context, const ::google::protobuf::Empty* request, ::google::protobuf::Empty* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::google::protobuf::Empty, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_EndStream_, context, request, response, std::move(f));
}

void DataService::Stub::async::EndStream(::grpc::ClientContext* context, const ::google::protobuf::Empty* request, ::google::protobuf::Empty* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_EndStream_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::PrepareAsyncEndStreamRaw(::grpc::ClientContext* context, const ::google::protobuf::Empty& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::google::protobuf::Empty, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_EndStream_, context, request);
}

::grpc::ClientAsyncResponseReader< ::google::protobuf::Empty>* DataService::Stub::AsyncEndStreamRaw(::grpc::ClientContext* context, const ::google::protobuf::Empty& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncEndStreamRaw(context, request, cq);
  result->StartCall();
  return result;
}

DataService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      DataService_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< DataService::Service, ::dataflow::Record, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](DataService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::dataflow::Record* req,
             ::google::protobuf::Empty* resp) {
               return service->SendRecord(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      DataService_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< DataService::Service, ::dataflow::RecordBatch, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](DataService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::dataflow::RecordBatch* req,
             ::google::protobuf::Empty* resp) {
               return service->SendRecordBatch(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      DataService_method_names[2],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< DataService::Service, ::google::protobuf::Empty, ::google::protobuf::Empty, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](DataService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::google::protobuf::Empty* req,
             ::google::protobuf::Empty* resp) {
               return service->EndStream(ctx, req, resp);
             }, this)));
}

DataService::Service::~Service() {
}

::grpc::Status DataService::Service::SendRecord(::grpc::ServerContext* context, const ::dataflow::Record* request, ::google::protobuf::Empty* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status DataService::Service::SendRecordBatch(::grpc::ServerContext* context, const ::dataflow::RecordBatch* request, ::google::protobuf::Empty* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status DataService::Service::EndStream(::grpc::ServerContext* context, const ::google::protobuf::Empty* request, ::google::protobuf::Empty* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace dataflow

