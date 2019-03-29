#include "persist_server.h"

#include <unordered_map>
#include <common/ds_config.h>

#include "frame/sf_logger.h"
#include "common/ds_config.h"
#include "common_thread.h"

namespace sharkstore {
namespace dataserver {
namespace server {

PersistServer::PersistServer() {}
PersistServer::PersistServer(const Options& ops) :
    ops_(ops) {
}

PersistServer::~PersistServer() {
    Stop();
}

int PersistServer::Init(ContextServer *context) {
    FLOG_INFO("PersistServer Init begin ...");

    context_ = context;

    Options ops;
    ops.thread_num = ds_config.persist_config.persist_threads;
    ops.delay_count = ds_config.persist_config.persist_delay_size;
    ops.queue_capacity = ds_config.persist_config.persist_queue_size;
    ops_ = ops;

    // 打开数据db
    if (OpenDB() != 0) {
        FLOG_ERROR("PersistServer Init error ...");
        return -1;
    }

    return 0;
}

Status PersistServer::Start() {

    for (uint64_t i = 0; i < ops_.thread_num; ++i) {
        auto t = new WorkThread( ops_.queue_capacity, std::string("storage-reader:") + std::to_string(i));
        threads_.emplace_back(t);
    }
    FLOG_INFO("persist[server] %" PRIu64 " storage reader threads start. queue capacity=%" PRIu64 ,
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

void PersistServer::PostPersist(const uint64_t range_id, const uint64_t persist, const uint64_t applied) {
    if (applied  - persist > ops_.delay_count) {
        readers_.find(range_id)->second->Notify(range_id, applied);
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

storage::IteratorInterface* PersistServer::GetIterator(const std::string& start, const std::string& limit) {
    if (!db_) {
        return nullptr;
    }

    return db_->NewIterator(start, limit);
}

Status PersistServer::CreateReader(const uint64_t range_id,
                                   std::function<bool(const std::string&, errorpb::Error *&err)> f0,
                                   std::function<bool(const metapb::RangeEpoch &, errorpb::Error *&err)> f1,
                                   std::shared_ptr<raft::RaftLogReader>* reader)
{
    auto idx = (range_id % threads_.size());
    *reader  = raft::CreateRaftLogReader(range_id, std::move(f0), std::move(f1),
                                         context_->raft_server, db_, threads_[idx]);
    //auto r = std::make_shared<sharkstore::raft::impl::StorageReader>(
    //        range_id, f0, f1, context_->raft_server, db_, threads_[idx]);
    readers_.emplace(std::make_pair(range_id, *reader));

    return Status::OK();
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
