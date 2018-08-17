#include "admin_server.h"

#include "net/session.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

using namespace ds_adminpb;

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
                onMessage(ctx, msg);
            });
    if (!ret.ok()) return ret;

    FLOG_INFO("[Admin] server listen on 0.0.0.0:%u", port);

    return Status::OK();
}

Status AdminServer::Stop() {
    return Status::OK();
}

Status AdminServer::checkAuth(const ds_adminpb::AdminAuth& auth) {
    // TODO:
    return Status::OK();
}

Status AdminServer::execute(const ds_adminpb::AdminRequest& req, ds_adminpb::AdminResponse* resp) {
    switch (req.typ()) {
        case SET_CONFIG:
            return setConfig(req.set_cfg_req(), resp->mutable_set_cfg_resp());
        case GET_CONFIG:
            return getConfig(req.get_cfg_req(), resp->mutable_get_cfg_resp());
        case GET_INFO:
            return getInfo(req.get_info_req(), resp->mutable_get_info_response());
        case FORCE_SPLIT:
            return forceSplit(req.force_split_req(), resp->mutable_force_split_resp());
        case COMPACTION:
            return compaction(req.compaction_req(), resp->mutable_compaction_resp());
        case CLEAR_QUEUE:
            return clearQueue(req.clear_queue_req(), resp->mutable_clear_queue_resp());
        case GET_PENDINGS:
            return getPending(req.get_pendings_req(), resp->mutable_get_pendings_resp());
        case FLUSH_DB:
            return flushDB(req.flush_db_req(), resp->mutable_flush_db_resp());
        default:
            return Status(Status::kNotSupported, "admin type", std::to_string(req.typ()));
    }
}

void AdminServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    AdminRequest req;
    if (!req.ParseFromArray(msg->body.data(), static_cast<int>(msg->body.size()))) {
        FLOG_ERROR("[Admin] deserialize failed from %s, head: %s",
                ctx.remote_addr.c_str(), msg->head.DebugString().c_str());
    }
    FLOG_INFO("[Admin] recv %s from %s.", ds_adminpb::AdminType_Name(req.typ()).c_str(), ctx.remote_addr.c_str());

    AdminResponse resp;
    Status ret = checkAuth(req.auth());
    if (ret.ok()) {
        ret = execute(req, &resp);
    }

    if (!ret.ok()) {
        FLOG_WARN("[Admin] handle %s from %s error: %s", AdminType_Name(req.typ()).c_str(),
                ctx.remote_addr.c_str(), ret.ToString().c_str());
        resp.set_code(static_cast<uint32_t>(ret.code()));
        resp.set_error_msg(ret.ToString());
    }

    auto resp_msg = net::NewMessage();
    resp_msg->head.SetFrom(msg->head);
    resp_msg->body.resize(resp.ByteSizeLong());
    resp.SerializeToArray(resp_msg->body.data(), static_cast<int>(resp_msg->body.size()));
    auto conn = ctx.session.lock();
    if (conn) {
        conn->Write(resp_msg);
    }
}


Status AdminServer::forceSplit(const ds_adminpb::ForceSplitRequest& req, ds_adminpb::ForceSplitResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::compaction(const ds_adminpb::CompactionRequest& req, ds_adminpb::CompactionResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::clearQueue(const ds_adminpb::ClearQueueRequest& req, ds_adminpb::ClearQueueResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::getPending(const ds_adminpb::GetPendingsRequest& req, ds_adminpb::GetPendingsResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::flushDB(const ds_adminpb::FlushDBRequest& req, ds_adminpb::FlushDBResponse* resp) {
    return Status(Status::kNotSupported);
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
