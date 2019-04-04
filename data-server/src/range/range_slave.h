_Pragma("once");

#include "range_base.h"
#include "server/common_thread.cpp"

namespace sharkstore {
namespace dataserver {
namespace range {

class RangeSlave : public RangeBase, public std::enable_shared_from_this<RangeSlave> {
public:
    RangeSlave(RangeContext* context, const metapb::Range &meta);
    ~RangeSlave();

    RangeSlave(const RangeSlave &) = delete;
    RangeSlave &operator=(const RangeSlave &) = delete;
    RangeSlave &operator=(const RangeSlave &) volatile = delete;

    Status Initialize(uint64_t leader = 0, uint64_t log_start_index = 0, uint64_t sflag = 0) override ;
    Status Shutdown() override ;

    Status Submit(const uint64_t range_id, const uint64_t pidx, const uint64_t aidx) override ;

    uint64_t PersistIndex() const noexcept { return persist_index_; }

private:
    bool tryPost(const std::function<void()>& f);
    Status dealTask();

private:
    volatile uint64_t persist_index_ = 0;

    raft::RaftLogReader* reader_ = nullptr;
    dataserver::WorkThread* trd_ = nullptr;
    std::atomic<bool> running_ = {false};
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
