#include "persist_server.h"

#include "frame/sf_logger.h"
#include "common/ds_config.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace server {

PersistServer::PersistServer(const Options& ops) :
    ops_(ops) {
}

PersistServer::~PersistServer() {
    Stop();
}

int PersistServer::Init(ContextServer *context) {
    FLOG_INFO("PersistServer Init begin ...");

    context_ = context;

    // 打开数据db
    if (OpenDB() != 0) {
        FLOG_ERROR("PersistServer Init error ...");
        return -1;
    }

    return 0;
}

Status PersistServer::Start() {

    return Status::OK();
}

Status PersistServer::Stop() {
    return Status::OK();
}

int PersistServer::OpenDB() {
    //print_rocksdb_config();
    db_ = new storage::RocksDBImpl(ds_config.async_rocksdb_config);

    auto s = db_->Open();
    if (!s.ok()) {
        FLOG_ERROR("open rocksdb failed: %s", s.ToString().c_str());
        return -1;
    } else {
        FLOG_INFO("open rocksdb successfully");
    }
    return 0;
}

void PersistServer::CloseDB() {
    if (db_ != nullptr) {
        delete db_;
        db_ = nullptr;
    }
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
