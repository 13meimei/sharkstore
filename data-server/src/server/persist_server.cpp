#include "persist_server.h"

#include <unordered_map>
#include <common/ds_config.h>

#include "frame/sf_logger.h"
#include "common/ds_config.h"

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
    meta_ = context->meta_store;
    pdb_ = context->pdb;

    Options ops;
    ops.thread_num = ds_config.persist_config.persist_threads;
    ops.delay_count = ds_config.persist_config.persist_delay_size;
    ops.queue_capacity = ds_config.persist_config.persist_queue_size;
    ops_ = ops;

    // 打开数据db
    if (pdb_ == nullptr && OpenDB() != 0) {
        FLOG_ERROR("PersistServer Init error ...");
        return -1;
    }

    //uint64_t idx;
    //LoadPersistIndex(range_id, &idx);
    //pindex_ = idx;
    return 0;
}

Status PersistServer::Start() {
    for (uint64_t i = 0; i < ops_.thread_num; ++i) {
        threads_.emplace_back(new WorkThread( ops_.queue_capacity,
                                              std::string("storage-reader:") + std::to_string(i)));
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
    threads_.clear();

    auto it = readers_.begin();
    while (it != readers_.end()) {
        it->second->Close();
        it = readers_.erase(it);
    }

    CloseDB();
    return Status::OK();
}

bool PersistServer::IndexInDistance(const uint64_t range_id, const uint64_t aidx, const uint64_t pidx) {
    if (aidx  - pidx > ops_.delay_count) {
        return true;
    }
    return false;
}

Status PersistServer::SavePersistIndex(const uint64_t range_id, const uint64_t persist_index) {
    pindex_ = persist_index;
    return meta_->SavePersistIndex(range_id, persist_index);
}

Status PersistServer::LoadPersistIndex(const uint64_t range_id, uint64_t* persist_index) {
    auto r = meta_->LoadPersistIndex(range_id, persist_index);
    pindex_ = *persist_index;
    return r;
}

int PersistServer::OpenDB() {
    //print_rocksdb_config();
    pdb_ = new storage::RocksDBImpl(ds_config.async_rocksdb_config);

    auto s = pdb_->Open();
    if (!s.ok()) {
        FLOG_ERROR("open rocksdb failed: %s", s.ToString().c_str());
        return -1;
    } else {
        FLOG_INFO("open rocksdb successfully");
    }
    return 0;
}

void PersistServer::CloseDB() {
    if (pdb_ != nullptr) {
        delete pdb_;
        pdb_ = nullptr;
    }
}

storage::IteratorInterface* PersistServer::GetIterator(const std::string& start, const std::string& limit) {
    if (!pdb_) {
        return nullptr;
    }

    return pdb_->NewIterator(start, limit);
}

Status PersistServer::GetWorkThread(const uint64_t range_id, WorkThread*& trd) {
    auto idx = (range_id % threads_.size());
    trd = threads_[idx];
    return Status::OK();
}

Status PersistServer::CreateReader(const uint64_t range_id,
                                   const uint64_t start_index,
                                   std::shared_ptr<raft::RaftLogReader>* reader)
{
    *reader  = raft::CreateRaftLogReader(range_id, start_index, context_->raft_server);
    readers_.emplace(std::make_pair(range_id, *reader));

    return Status::OK();
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
