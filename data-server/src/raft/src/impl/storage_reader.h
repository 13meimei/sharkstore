_Pragma("once");

#include <atomic>
#include <memory>
#include <functional>
#include <map>
#include <queue>
//#include <unordered_map>
//#include <mutex>
//#include <condition_variable>

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
namespace test { class StorageTest; }

namespace raft {
namespace impl {


class StorageReader : public RaftLogReader,  public std::enable_shared_from_this<StorageReader> {
public:
    StorageReader(const uint64_t id, const uint64_t index, RaftServer *server);
    ~ StorageReader() override ;

    StorageReader(const StorageReader&) = delete;
    StorageReader& operator=(const StorageReader&) = delete;

    Status GetData(const uint64_t idx, std::shared_ptr<raft_cmdpb::Command>& cmd) override;
    Status Close() override;

private:
    Status getCurrLogFile(const uint64_t idx);
    void appliedTo(uint64_t idx) noexcept { applied_ = idx; }

    using EntryPtr = std::shared_ptr<Entry>;
    Status decodeEntry(EntryPtr entry, std::shared_ptr<raft_cmdpb::Command>&);
    Status listLogs();

private:
    uint64_t id_{0};
    uint64_t start_index_{0};
    uint64_t applied_{0};

    sharkstore::raft::RaftServer* server_ = nullptr;
    //std::vector<std::shared_ptr<LogFile>> log_files_;
    std::queue<std::shared_ptr<LogFile>> log_files_;
    std::shared_ptr<LogFile> curr_log_file_ = nullptr;
#ifndef NDEBUG
public:
    friend class sharkstore::test::StorageTest;
#endif
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
