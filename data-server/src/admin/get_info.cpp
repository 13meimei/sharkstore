#include "admin_server.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

Status AdminServer::getInfo(const ds_adminpb::GetInfoRequest& req, ds_adminpb::GetInfoResponse* resp) {
    return Status(Status::kNotSupported);
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore

