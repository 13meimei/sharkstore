#include "raft/raft_log_reader.h"
#include "impl/storage_reader.h"

namespace sharkstore {
namespace raft {

std::shared_ptr<RaftLogReader> CreateRaftLogReader(const uint64_t id,
                                                   const uint64_t start_idx,
                                                   RaftServer *server)
{
    return std::shared_ptr<RaftLogReader>(new impl::StorageReader(id, start_idx, server));
}

} /* namespace raft */
} /* namespace sharkstore */
