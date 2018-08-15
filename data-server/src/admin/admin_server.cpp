#include "admin_server.h"

#include "proto/gen/ds_admin.pb.h"
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
            [this](const net::Context& ctx, const net::Head& head, std::vector<uint8_t>&& body) {
        OnMessage(ctx, head, std::move(body));
    });
    if (!ret.ok()) return ret;

    FLOG_INFO("[Admin] server listen on 0.0.0.0:%u", port);

    return Status::OK();
}

Status AdminServer::Stop() {
    return Status::OK();
}

void AdminServer::OnMessage(const net::Context& ctx, const net::Head& head,
               std::vector<uint8_t>&& body) {
    ds_adminpb::AdminRequest req;
    if (!req.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
        FLOG_ERROR("[Admin] deserialize failed from %s, head: %s",
                ctx.remote_addr.c_str(), head.DebugString().c_str());
    }
    FLOG_INFO("[Admin] recv %s from %s.", ds_adminpb::AdminType_Name(req.typ()).c_str(), ctx.remote_addr.c_str());

    ds_adminpb::AdminResponse resp;
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
