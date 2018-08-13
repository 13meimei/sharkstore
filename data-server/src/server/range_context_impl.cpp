#include "range_context_impl.h"

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "range_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

uint64_t RangeContextImpl::SplitSize() const {
    return ds_config.range_config.split_size;
}

uint64_t RangeContextImpl::MaxSize() const {
    return ds_config.range_config.max_size;
}

void RangeContextImpl::ScheduleHeartbeat(uint64_t range_id, bool delay) {
    auto expire = delay ? ds_config.hb_config.range_interval * 1000 + getticks() :
            getticks();
    server_->range_server->LeaderQueuePush(range_id, expire);
}

void RangeContextImpl::ScheduleCheckSize(uint64_t range_id) {
    server_->range_server->StatisPush(range_id);
}

std::shared_ptr<range::Range> RangeContextImpl::FindRange(uint64_t range_id) {
    return server_->range_server->Find(range_id);
}

// split
Status RangeContextImpl::SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req,
                  uint64_t raft_index, std::shared_ptr<range::Range> *new_range) {
    return Status(Status::kNotFound);
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
