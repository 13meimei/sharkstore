_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/db_interface.h"
#include "meta_file.h"
#include "storage.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {
    static const std::string kPersistRaftLogPrefix = "\x06";
}}}

namespace sharkstore {
namespace raft {
namespace impl {
    class RaftServerImpl;
}}}

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class StorageReader {
public:
    StorageReader(const uint64_t id,
              raft::impl::RaftServerImpl *server,
              const std::string& path,
              dataserver::storage::DbInterface* db);
    ~ StorageReader();

    StorageReader(const StorageReader&) = delete;
    StorageReader& operator=(const StorageReader&) = delete;

    Status GetCommitFiles();
    Status ProcessFiles();

    Status ApplyRaftCmd(const raft_cmdpb::Command& cmd);
    Status ApplyIndex(uint64_t index);
    Status ApplySnapshot(const pb::SnapshotMeta&meta);

    Status Close();

private:
    std::unique_ptr<raft_cmdpb::Command> decodeEntry(EntryPtr entry);
    Status storeRawPut(const raft_cmdpb::Command &cmd);
    Status saveApplyIndex(uint64_t range_id, uint64_t apply_index);

    Status listLogs();
private:
    uint64_t id_{0};
    uint64_t applied_{0};
    uint64_t curr_seq_{0};
    uint64_t curr_index_{0};
    std::string path_{""};

    raft::impl::RaftServerImpl* server_ = nullptr;
    std::shared_ptr<impl::storage::Storage> storage_ = nullptr;

    std::vector<std::shared_ptr<LogFile>> log_files_;
    //rocksdb
    dataserver::storage::DbInterface* db_ == nullptr;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
