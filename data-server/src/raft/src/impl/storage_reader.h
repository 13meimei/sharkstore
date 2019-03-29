_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include <functional>
//#include <mutex>
//#include <condition_variable>
#include <vector>
#include <map>

#include "raft/raft_log_reader.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/db_interface.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

namespace sharkstore {
    class Status;
namespace dataserver {
    class WorkThread;
namespace storage {
    //need move to meta_store.cpp
    static const std::string kStorageRangePersistPrefix = "\x06";

    class DbInterface;
}}}

using Status = sharkstore::Status;
using WorkThread =  sharkstore::dataserver::WorkThread;
using RaftServer = sharkstore::raft::RaftServer;
using LogFile = sharkstore::raft::impl::storage::LogFile;
using Entry = sharkstore::raft::impl::pb::Entry;
using DbInterface = sharkstore::dataserver::storage::DbInterface;
using RaftImpl =  sharkstore::raft::impl::RaftImpl;

namespace sharkstore {
namespace raft {
namespace impl {

class StorageReader : public RaftLogReader,  public std::enable_shared_from_this<StorageReader> {
public:
    StorageReader(const uint64_t id,
              const std::function<bool(const std::string&, errorpb::Error *&err)>& f0,
              const std::function<bool(const metapb::RangeEpoch &meta, errorpb::Error *&err)>& f1,
              RaftServer *server,
              DbInterface* db,
              WorkThread* trd);
    ~ StorageReader() override ;

    StorageReader(const StorageReader&) = delete;
    StorageReader& operator=(const StorageReader&) = delete;

    Status DealTask() override ;
    size_t GetCommitFiles() override ;
    Status ProcessFiles() override ;

    Status ApplyRaftCmd(const raft_cmdpb::Command& cmd) override ;

    Status AppliedTo(uint64_t index) override ;
    Status LoadApplied(uint64_t *index) override ;
    //Status ApplySnapshot(const pb::SnapshotMeta&meta);

    uint64_t Applied()  override;

    //index: range applied index
    Status Notify(const uint64_t range_id, const uint64_t index) override ;
    Status Close() override ;
    //TO DO 后续改造reader仅负责解析文件，读取文件（读取完毕切换），并返回raft command结构
    //持久化放在persist server中调度，双range结构
    Status Next(uint64_t& index, std::shared_ptr<raft_cmdpb::Command>& cmd);

private:
    void init();
    using EntryPtr = std::shared_ptr<Entry>;
    std::shared_ptr<raft_cmdpb::Command> decodeEntry(EntryPtr entry);

    Status storeRawPut(const raft_cmdpb::Command &cmd);
    Status storeRawDelete(const raft_cmdpb::Command &cmd);

    Status storeInsert(const raft_cmdpb::Command & cmd);
    Status storeUpdate(const raft_cmdpb::Command & cmd);
    Status storeDelete(const raft_cmdpb::Command & cmd);

    Status storeKVSet(const raft_cmdpb::Command & cmd);
    Status storeKVBatchSet(const raft_cmdpb::Command & cmd);
    Status storeKVDelete(const raft_cmdpb::Command & cmd);
    Status storeKVBatchDelete(const raft_cmdpb::Command & cmd);
    Status storeKVRangeDelete(const raft_cmdpb::Command & cmd);

    Status storeDBGet(const std::string &key, std::string * value);
    Status storeDBPut(const std::string &key, const std::string & value);
    Status storeDBDelete(const std::string &key);
    Status storeDBInsert(const kvrpcpb::InsertRequest &req, uint64_t * affected);
    Status storeDBUpdate(const kvrpcpb::UpdateRequest &req, uint64_t * affected, uint64_t update_bytes);

    Status saveApplyIndex(const uint64_t range_id, const uint64_t apply_index);
    Status restoreAppliedIndex(const uint64_t range_id, uint64_t* apply_index);

    bool tryPost(const std::function<void()>& f);
    Status listLogs();


    std::function<bool(const std::string& key, errorpb::Error *&err)> keyInRange;
    std::function<bool(const metapb::RangeEpoch &epoch, errorpb::Error *&err)> EpochIsEqual;

private:
    std::atomic<bool> init_flag = {false};
    uint64_t id_{0};
    uint64_t applied_{0};
    uint64_t curr_seq_{0};
    uint64_t curr_index_{0};

    sharkstore::raft::RaftServer* server_ = nullptr;

    std::vector<std::shared_ptr<LogFile>> log_files_;
    //<seq, applied_index>
    std::map<uint64_t, uint64_t> done_files_;
    //rocksdb
    DbInterface* db_ = nullptr;
    std::atomic<bool> running_ = {false};
    WorkThread* trd_ = nullptr;

    //std::mutex  mtx_;
    //std::condition_variable cond_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
