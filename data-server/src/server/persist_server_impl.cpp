#include "persist_server.h"
#include "persist_server_impl.h"

#include "frame/sf_logger.h"
#include "common/ds_config.h" 
#include "storage/db/rocksdb_impl/rocksdb_impl.h"

namespace sharkstore {
namespace dataserver {
namespace server {


PersistServerImpl::PersistServerImpl(const PersistOptions& ops) :
    ops_(ops) 
{
}

PersistServerImpl::~PersistServerImpl() 
{
    Stop();
}

int PersistServerImpl::Init(ContextServer *context) 
{
    FLOG_INFO("PersistServerImpl Init begin ...");

    context_ = context;
    if (!context_) { 
        FLOG_ERROR("PersistServerImpl Init context is null.");
        return -1;
    }

    // 打开数据db
    if (OpenDB() != 0) {
        FLOG_ERROR("PersistServerImpl Init OpenDB error ...");
        return -1;
    }
    
    context_->pdb = pdb_;

    return 0;
}

Status PersistServerImpl::Start() 
{
    for (uint64_t i = 0; i < ops_.thread_num; ++i) {
        FLOG_DEBUG("Create WorkThread %" PRIu64 "...", i);
        threads_.emplace_back(new WorkThread( ops_.queue_capacity,
                                              std::string("storage-reader:") + std::to_string(i)));
    }
    FLOG_INFO("persist[server] %" PRIu64 " storage reader threads start. queue capacity=%" PRIu64 ,
                  ops_.thread_num, ops_.queue_capacity);

    running_ = true;
    return Status::OK();
}

Status PersistServerImpl::Stop() 
{
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

bool PersistServerImpl::IndexInDistance(const uint64_t range_id, const uint64_t apply_id, const uint64_t persist_id)
{
    FLOG_DEBUG("---range_index: %" PRIu64 "---apply_index: %" PRIu64 " persist_index: %" PRIu64 " delay_count: %" PRIu64 "---",
            range_id, apply_id, persist_id, ops_.delay_count);
    if (apply_id - persist_id >= ops_.delay_count) {
        return true;
    }
    return false;
}

int PersistServerImpl::OpenDB() 
{
    print_async_rocksdb_config();
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

void PersistServerImpl::CloseDB() 
{
    if (pdb_ != nullptr) {
        delete pdb_;
        pdb_ = nullptr;
    }
}

storage::IteratorInterface* PersistServerImpl::GetIterator(const std::string& start, const std::string& limit) 
{
    if (!pdb_) {
        return nullptr;
    }

    return pdb_->NewIterator(start, limit);
}

Status PersistServerImpl::GetWorkThread(const uint64_t range_id, WorkThread*& trd) 
{
    auto idx = (range_id % threads_.size());
    trd = threads_[idx];
    return Status::OK();
}

Status PersistServerImpl::CreateReader(const uint64_t range_id,
                                   const uint64_t start_index,
                                   std::shared_ptr<raft::RaftLogReader>* reader)
{
    *reader  = raft::CreateRaftLogReader(range_id, start_index, context_?context_->raft_server:nullptr);
    readers_.emplace(std::make_pair(range_id, *reader));

    return Status::OK();
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */