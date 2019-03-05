_Pragma("once");

#include "raft/snapshot.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "iterator_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Snapshot : public raft::Snapshot {
public:
    Snapshot(uint64_t applied, std::string&& context,
            std::unique_ptr<IteratorInterface> data_iter,
            std::unique_ptr<IteratorInterface> txn_iter);

    ~Snapshot();

    Status Context(std::string* context) override;
    uint64_t ApplyIndex() override { return applied_; }

    Status Next(std::string* data, bool* over) override;
    void Close() override;

private:
    Status next(IteratorInterface* iter, raft_cmdpb::CFType type, std::string* data, bool *over);

private:
    uint64_t applied_ = 0;
    std::string context_;
    std::unique_ptr<IteratorInterface> data_iter_;
    std::unique_ptr<IteratorInterface> txn_iter_;
    bool data_over_ = false;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
