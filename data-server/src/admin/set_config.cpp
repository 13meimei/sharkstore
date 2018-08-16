#include "admin_server.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

Status AdminServer::setConfig(const ds_adminpb::SetConfigRequest& req, ds_adminpb::SetConfigResponse* resp) {
    return Status(Status::kNotSupported);
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore

