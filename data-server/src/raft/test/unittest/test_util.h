_Pragma("once");

#include "base/status.h"
#include "raft/src/impl/raft_types.h"

namespace fbase {
namespace raft {
namespace impl {
namespace testutil {

Status Equal(const pb::HardState& lh, const pb::HardState& rh);
Status Equal(const pb::TruncateMeta& lh, const pb::TruncateMeta& rh);

EntryPtr RandEntry(uint64_t index, int data_size = 64);
void RandEntries(uint64_t lo, uint64_t hi, int data_size,
                 std::vector<EntryPtr>* entries);
Status Equal(const EntryPtr& lh, const EntryPtr& rh);
Status Equal(const std::vector<EntryPtr>& lh, const std::vector<EntryPtr>& rh);

} /* namespace testutil */
} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
