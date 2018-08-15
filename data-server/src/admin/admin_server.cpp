#include "admin_server.h"

#include "proto/gen/ds_admin.pb.h"
#include "net/session.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

AdminServer::AdminServer(server::ContextServer* context) :
    context_(context) {
}

AdminServer::~AdminServer() {
    Stop();
}

Status AdminServer::Start(uint16_t port) {
    net::ServerOptions sops;
    sops.io_threads_num = 0;
    sops.max_connections = 200;
    net_server_.reset(new net::Server(sops));

    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
            [this](const net::Context& ctx, const net::MessagePtr& msg) {
                OnMessage(ctx, msg);
            });
    if (!ret.ok()) return ret;

    FLOG_INFO("[Admin] server listen on 0.0.0.0:%u", port);

    return Status::OK();
}

Status AdminServer::Stop() {
    return Status::OK();
}

void AdminServer::OnMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    ds_adminpb::AdminRequest req;
    if (!req.ParseFromArray(msg->body.data(), static_cast<int>(msg->body.size()))) {
        FLOG_ERROR("[Admin] deserialize failed from %s, head: %s",
                ctx.remote_addr.c_str(), msg->head.DebugString().c_str());
    }
    FLOG_INFO("[Admin] recv %s from %s.", ds_adminpb::AdminType_Name(req.typ()).c_str(), ctx.remote_addr.c_str());

    ds_adminpb::AdminResponse resp;
    resp.set_code(12);
    resp.set_error_msg("not implemented");

    auto resp_msg = net::NewMessage();
    resp_msg->head.SetFrom(msg->head);
    resp_msg->body.resize(resp.ByteSizeLong());
    resp.SerializeToArray(resp_msg->body.data(), static_cast<int>(resp_msg->body.size()));

    auto conn = ctx.session.lock();
    if (conn) {
        conn->Write(resp_msg);
    }
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
