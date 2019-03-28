#include "raft/raft_log_reader.h"
#include "impl/storage_reader.h"

namespace sharkstore {
namespace raft {

std::shared_ptr<RaftLogReader> CreateRaftLogReader(const uint64_t id,
                                                   const std::function<bool(const std::string&)>& f0,
                                                   const std::function<bool(const metapb::Range &meta)>& f1,
                                                   RaftServer *server,
                                                   sharkstore::dataserver::storage::DbInterface* db,
                                                   sharkstore::raft::impl::WorkThread* trd)
{
    return std::shared_ptr<RaftLogReader>(new impl::StorageReader(id, f0, f1, server, db, trd));
}

} /* namespace raft */
} /* namespace sharkstore */
