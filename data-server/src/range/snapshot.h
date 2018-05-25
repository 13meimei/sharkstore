#ifndef FBASE_DATASERVER_RANGE_SNAPSHOT_H_
#define FBASE_DATASERVER_RANGE_SNAPSHOT_H_

#include "proto/gen/raft_cmdpb.pb.h"
#include "raft/snapshot.h"
#include "storage/iterator.h"

namespace fbase {
namespace dataserver {
namespace range {

class Snapshot : public raft::Snapshot {
public:
    Snapshot(uint64_t applied, raft_cmdpb::SnapshotContext&& ctx,
             storage::Iterator* iter);
    ~Snapshot();

    Status Next(std::string* data, bool* over) override;
    Status Context(std::string* context) override;
    uint64_t ApplyIndex() override;
    void Close() override;

private:
    uint64_t applied_ = 0;
    raft_cmdpb::SnapshotContext context_;
    storage::Iterator* iter_ = nullptr;
};

} /* namespace range */
} /* namespace dataserver */
} /* namespace fbase */

#endif /* end of include guard: FBASE_DATASERVER_RANGE_SNAPSHOT_H_ */