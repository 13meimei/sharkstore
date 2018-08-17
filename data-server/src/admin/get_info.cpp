#include "admin_server.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

std::string getSummaryInfo(server::ContextServer* context) {
    return "";
}

Status AdminServer::getInfo(const ds_adminpb::GetInfoRequest& req, ds_adminpb::GetInfoResponse* resp) {
    if (req.path().empty()) {
        resp->set_data(getSummaryInfo(context_));
    }
    return Status::OK();
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore

