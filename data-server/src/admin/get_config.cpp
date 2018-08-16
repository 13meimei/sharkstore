#include "admin_server.h"

#include <map>
#include <string>
#include <functional>

#include <fastcommon/logger.h>

#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

using namespace ds_adminpb;

using ConfigGeterMap = std::map<std::string, std::function<std::string()>> ;

static const ConfigGeterMap cfg_getters = {
        // log
        {"log.level", []{ return std::string(get_log_level_name(g_log_context.log_level)); }}
        //
};

static bool getConfigValue(const ConfigKey& key, std::string* value) {
    auto it = cfg_getters.find(key.section() + "." + key.name());
    if (it == cfg_getters.cend()) {
        return false;
    }
    *value = (it->second)();
    return true;
}

Status AdminServer::getConfig(const ds_adminpb::GetConfigRequest& req, ds_adminpb::GetConfigResponse* resp) {
    for (const auto& key: req.key()) {
        auto cfg = resp->add_configs();
        cfg->mutable_key()->CopyFrom(key);
        if(!getConfigValue(key, cfg->mutable_value())) {
            return Status(Status::kNotFound, "config key", key.section() + "." + key.name());
        }
    }
    return Status::OK();
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
