#include "range_slave.h"

#include <cinttypes>
#include "range_logger.h"
#include "server/persist_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

RangeSlave::RangeSlave(RangeContext* context, const metapb::Range &meta) : RangeBase(context, meta) {}
RangeSlave::~RangeSlave() { Shutdown(); }

Status RangeSlave::Initialize(uint64_t leader, uint64_t log_start_index, uint64_t sflag) {
    // 加载persist位置
    auto s = context_->MetaStore()->LoadPersistIndex(id_, &persist_index_);
    if (!s.ok()) {
        return Status(Status::kCorruption, "load applied", s.ToString());
    }

    //auto ret = RangeBase::Initialize(leader, log_start_index, sflag);
    auto ret = RangeBase::Initialize(leader, persist_index_, sflag);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("RangeSlave::Initialize failed, code:%d, msg:%s", ret.code(),
                        ret.ToString().c_str());
        return ret;
    }

    //acquire raft log reader object
    std::shared_ptr<raft::RaftLogReader> reader = nullptr;
    context_->PersistServer()->CreateReader(id_, persist_index_, &reader);
    reader_ = reader.get();

    //acquire work thread
    ret = context_->PersistServer()->GetWorkThread(id_, trd_);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("RangeSlave::Initialize failed, code:%d, msg:%s", ret.code(),
                        ret.ToString().c_str());
        return ret;
    }
    assert(trd_ != nullptr);
    running_ = true;
    return ret;
}

Status RangeSlave::Shutdown() {
    RangeBase::Shutdown();
    running_ = false;
    return Status::OK();
}

Status RangeSlave::Submit(const uint64_t range_id, const uint64_t pidx, const uint64_t aidx) {
    RANGE_LOG_DEBUG("Submit, Notify to persist, range_id: %" PRIu64  " persist index: %" PRIu64
                            " applied index: %" PRIu64 " running: %s", range_id, pidx, aidx, running_?"yes":"no");

    if (!running_) {
        return Status(Status::kShutdownInProgress, "server is stopping",
                      std::to_string(id_));
    }
    //if(!context_->PersistServer()->IndexInDistance(range_id, aidx, pidx)) {
    if(!context_->PersistServer()->IndexInDistance(range_id, aidx, persist_index_)) {
        RANGE_LOG_DEBUG("not in distance");
        return Status::OK();
    }

    if(!tryPost(std::bind(&RangeSlave::dealTask, shared_from_this()))) {
        RANGE_LOG_ERROR("Notify to persist fail, maybe overlimit capacity, range_id: %" PRIu64
                                " persist index: %" PRIu64, range_id, persist_index_);
    }
    return Status::OK();
}

bool RangeSlave::tryPost(const std::function<void()>& f) {
    Work w;
    w.owner = id_;
    w.running = &running_;
    w.f0 = f;
    return trd_->tryPost(w);
}

Status RangeSlave::dealTask() {
    Status ret;
    uint64_t idx = persist_index_;
    RANGE_LOG_DEBUG("RangeSlave dealTask()...");
    do {
        if (!running_) break;

        //to do read from raft log and Apply
        std::shared_ptr<raft_cmdpb::Command> cmd = nullptr;
        ret = reader_->GetData(++idx, cmd);
        if (!ret.ok()) {
            break;
        }

        ret = RangeBase::Apply(*cmd, idx);
//        if (!ret.ok()) {
//            break;
//        }

        ret = context_->MetaStore()->SavePersistIndex(id_, idx);
        if (!ret.ok()) {
            break;
        }
        persist_index_ = idx;

    } while (true);

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore


