#include "rpc_server.h"

#include "common/ds_config.h"
#include "frame/sf_logger.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "net/session.h"
#include "storage/metric.h"

namespace sharkstore {
namespace dataserver {
namespace server {

RPCServer::RPCServer(const net::ServerOptions& ops) :
    ops_(ops) {
    net_server_(new net::Server(ops, "rpc")) {
}

RPCServer::~RPCServer() {
}


Status RPCServer::Start(const std::string& ip, uint16_t port, const net::Handler& handler) {

}
Status RPCServer::Start() {
    net::ServerOptions sops;
    sops.max_connections = 10000; // TODO
    sops.io_threads_num = static_cast<size_t>(ds_config.worker_config.event_recv_threads);
    net_server_.reset(new net::Server(sops));

    auto port = static_cast<uint16_t>(ds_config.worker_config.port);
    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
                                           [this](const net::Context& ctx, const net::MessagePtr& msg) {
                                               onMessage(ctx, msg);
                                           });
    if (!ret.ok()) return ret;

    FLOG_INFO("RPC Server listen on 0.0.0.0:%u", port);

    return Status::OK();
}

Status RPCServer::Stop() {
    if (net_server_) {
        net_server_->Stop();
    }
    FLOG_INFO("RPC Server stopped");
    return Status::OK();
}

void RPCServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    auto func_id = static_cast<funcpb::FunctionID>(msg->head.func_id);
    switch (func_id) {
        case funcpb::kFuncInsert:
            insert(ctx, msg);
            break;
        case funcpb::kFuncSelect:
            select(ctx, msg);
            break;
        default:
            FLOG_WARN("unsupported func type: %s, from %s",
                    funcpb::FunctionID_Name(func_id).c_str(), ctx.remote_addr.c_str());
    }
}

void RPCServer::reply(const net::Context& ctx, const net::Head& req_head,
           const ::google::protobuf::Message& resp) {
    auto resp_msg = net::NewMessage();
    resp_msg->head.SetResp(req_head);
    resp_msg->body.resize(resp.ByteSizeLong());
    resp.SerializeToArray(resp_msg->body.data(), static_cast<int>(resp_msg->body.size()));
    auto conn = ctx.session.lock();
    if (conn) {
        conn->Write(resp_msg);
    } else {
        FLOG_ERROR("reply failed to %s failed: connection closed", ctx.remote_addr.c_str());
    }
}

void RPCServer::insert(const net::Context& ctx, const net::MessagePtr& msg) {
    kvrpcpb::DsInsertResponse resp;
    resp.mutable_resp()->set_affected_keys(1);
    reply(ctx, msg->head, resp);

    storage::g_metric.AddWrite(1, 1);
}

void RPCServer::select(const net::Context& ctx, const net::MessagePtr& msg) {
    kvrpcpb::DsSelectResponse resp;
    reply(ctx, msg->head, resp);

    storage::g_metric.AddRead(1, 1);
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
