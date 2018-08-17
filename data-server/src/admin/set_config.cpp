#include "admin_server.h"

#include <fastcommon/shared_func.h>
#include <common/ds_config.h>
#include "frame/sf_logger.h"
#include "common/ds_config.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

using namespace ds_adminpb;

using ConfigSetFunc = std::function<Status(server::ContextServer*, const std::string&)>;
using ConfigSetFuncMap = std::map<std::string, ConfigSetFunc>;

Status setLogLevel(server::ContextServer* ctx, const std::string& value) {
    (void)ctx;
    char level[8] = {'\0'};
    snprintf(level, 8, "%s", value.c_str());
    set_log_level(level);
    return Status::OK();
}

Status setRangeCheckSize(server::ContextServer* ctx, const std::string& value) {
    (void)ctx;
    uint64_t new_value = 0;
    try {
        new_value = std::stoull(value);
    } catch (std::exception &e) {
        return Status(Status::kInvalidArgument, "range check size", value);
    }
    ds_config.range_config.check_size = new_value;
    return Status::OK();
}

Status setRangeSplitSize(server::ContextServer* ctx, const std::string& value) {
    (void)ctx;
    uint64_t new_value = 0;
    try {
        new_value = std::stoull(value);
    } catch (std::exception &e) {
        return Status(Status::kInvalidArgument, "range split size", value);
    }
    ds_config.range_config.split_size = new_value;
    return Status::OK();
}

Status setRangeMaxSize(server::ContextServer* ctx, const std::string& value) {
    (void)ctx;
    uint64_t new_value = 0;
    try {
        new_value = std::stoull(value);
    } catch (std::exception &e) {
        return Status(Status::kInvalidArgument, "range max size", value);
    }
    ds_config.range_config.max_size = new_value;
    return Status::OK();
}

static const ConfigSetFuncMap cfg_set_funcs = {
        {"log.level", setLogLevel },
        {"range.check_size", setRangeCheckSize },
        {"range.split_size", setRangeSplitSize },
        {"range.max_size", setRangeMaxSize },
};

Status AdminServer::setConfig(const SetConfigRequest& req, SetConfigResponse* resp) {
    for (auto &cfg: req.configs()) {
        auto it = cfg_set_funcs.find(cfg.key().section() + "." + cfg.key().name());
        if (it == cfg_set_funcs.end()) {
            return Status(Status::kNotSupported, "set config",
                    cfg.key().section() + "." + cfg.key().name());
        }
        auto s = (it->second)(context_, cfg.value());
        if (!s.ok()) {
            FLOG_WARN("[Admin] config %s.%s set to %s failed: %s", cfg.key().section().c_str(),
                      cfg.key().name().c_str(), cfg.value().c_str(), s.ToString().c_str());
            return s;
        }
        FLOG_INFO("[Admin] config %s.%s set to %s.", cfg.key().section().c_str(),
                cfg.key().name().c_str(), cfg.value().c_str());
    }
    return Status::OK();
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore

