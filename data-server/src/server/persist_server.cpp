#include "persist_server.h"

#include <unordered_map>
#include <src/raft/src/impl/storage/storage_reader.h>

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

    for (int i = 0; i < ops_.thread_num; ++i) {
        auto t = new sharkstore::raft::impl::WorkThread(this, ops_.queue_capacity,
                                std::string("storage-reader:") + std::to_string(i));
        threads_.emplace_back(t);
    }
    FLOG_INFO("persist[server] %d storage reader threads start. queue capacity=%d",
                  ops_.thread_num, ops_.queue_capacity);

    running_ = true;
    return Status::OK();
}

Status PersistServer::Stop() {
    if (!running_) return Status::OK();

    running_ = false;
    for (auto& t : threads_) {
        t->shutdown();
    }

    CloseDB();
    return Status::OK();
}

void PersistServer::TriggerPersist(const uint64_t range_id, const uint64_t persist, const uint64_t applied) {
    //TO DO get persist index
    if (applied  - persist > ops_.delay_count) {
        readers_[range_id]->Notify(range_id, applied);
    }
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

Status PersistServer::CreateReader(const uint64_t range_id,
                                   std::function<bool(const std::string&)> f0,
                                   std::function<bool(const metapb::Range &meta)> f1,
                                   std::shared_ptr<StorageReader>* reader)
{
    auto idx = (range_id % threads_.size());
    auto r = std::make_shared<sharkstore::raft::impl::storage::StorageReader>(
            range_id, f0, f1, context_->raft_server, db_, threads_);
    readers_.emplace(std::make_pair(range_id, r));
    *reader = r;

    return Status::OK();
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
