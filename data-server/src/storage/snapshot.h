_Pragma("once");

#include "raft/snapshot.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Snapshot : public raft::Snapshot {
public:
    Snapshot(uint64_t applied, std::string&& context,
            Iterator *data_iter, Iterator *txn_iter);

    ~Snapshot();

    Status Context(std::string* context) override;
    uint64_t ApplyIndex() override { return applied_; }

    Status Next(std::string* data, bool* over) override;
    void Close() override;

private:
    Status next(Iterator* iter, raft_cmdpb::CFType type, std::string* data, bool *over);

private:
    uint64_t applied_ = 0;
    std::string context_;
    Iterator* data_iter_ = nullptr;
    bool data_over_ = false;
    Iterator* txn_iter_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
