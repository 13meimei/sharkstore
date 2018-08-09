#include <common/ds_config.h>
#include "range.h"

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "master/worker.h"
#include "server/range_server.h"
#include "server/server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::CheckSplit(uint64_t size) {
    statis_size_ += size;

    if (!statis_flag_ && statis_size_ > ds_config.range_config.check_size) {
        statis_flag_ = true;
        context_->range_server->StatisPush(id_);
    }
}

void Range::ResetStatisSize() {
    // split size is split size, not half of split size
    // amicable sequence writing and random writing
    // const static uint64_t split_size = ds_config.range_config.split_size >>
    // 1;
    const static uint64_t split_size = ds_config.range_config.split_size;

    auto *meta = new metapb::Range;
    meta_.Get(meta);

    std::string split_key;
    if (ds_config.range_config.access_mode == 0) {
        real_size_ = store_->StatisSize(split_key, split_size);
    } else {
        real_size_ = store_->StatisSize(split_key, split_size, true);
    }

    RANGE_LOG_DEBUG("real size: %" PRIu64, real_size_);

    statis_flag_ = false;

    do {
        if (!EpochIsEqual(meta->range_epoch())) {
            RANGE_LOG_WARN("ResetStatisSize epoch is changed");
            break;
        }

        statis_size_ = 0;
        // when real size >= max size, we need split with split size
        if (real_size_ >= ds_config.range_config.max_size) {
            return AskSplit(split_key, meta);
        }
    } while (false);

    delete meta;
}

void Range::AskSplit(std::string &key, metapb::Range *meta) {
    assert(!key.empty());
    assert(key >= meta->start_key());
    assert(key < meta->end_key());

    RANGE_LOG_DEBUG("ask split %" PRIu64, id_);
    mspb::AskSplitRequest ask;
    ask.set_allocated_range(meta);
    ask.set_split_key(std::move(key));

    context_->master_worker->AsyncAskSplit(ask);
}

void Range::ReportSplit(const metapb::Range &new_range) {
    mspb::ReportSplitRequest report;
    meta_.Get(report.mutable_left());
    report.set_allocated_right(new metapb::Range(new_range));
    context_->master_worker->AsyncReportSplit(report);
}

void Range::AdminSplit(mspb::AskSplitResponse &resp) {
    if (!EpochIsEqual(resp.range().range_epoch())) {
        RANGE_LOG_WARN("AdminSplit epoch is changed");
        return;
    }

    auto &split_key = resp.split_key();

    RANGE_LOG_INFO(
        "AdminSplit new_range_id: %" PRIu64 " split_key: %s",
        resp.new_range_id(), EncodeToHexString(split_key).c_str());

    raft_cmdpb::Command cmd;
    uint64_t seq_id = submit_seq_.fetch_add(1);
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(seq_id);
    // set cmd_type
    cmd.set_cmd_type(raft_cmdpb::CmdType::AdminSplit);

    // set cmd admin_split_req
    auto split_req = cmd.mutable_admin_split_req();

    split_req->set_split_key(split_key);

    auto range = resp.release_range();

    auto psize = range->peers_size();
    auto rsize = resp.new_peer_ids_size();
    if (psize != rsize) {
        RANGE_LOG_WARN("AdminSplit peers_size no equal");
        return;
    }

    auto epoch = range->mutable_range_epoch();

    // set verify epoch
    cmd.set_allocated_verify_epoch(new metapb::RangeEpoch(*epoch));

    // set leader
    split_req->set_leader(node_id_);
    // set epoch
    split_req->mutable_epoch()->set_version(epoch->version() + 1);

    // no need set con_ver;con_ver is member change
    // split_req->mutable_epoch()->set_version(epoch->conf_ver());

    // set range id
    range->set_id(resp.new_range_id());
    // set range start_key
    range->set_start_key(split_key);
    // range end_key doesn't need to change.

    // set range_epoch
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    auto p0 = range->mutable_peers(0);
    // set range peers
    for (int i = 0; i < psize; i++) {
        auto px = range->mutable_peers(i);
        px->set_id(resp.new_peer_ids(i));

        // Don't consider the role
        if (i > 0 && px->node_id() == node_id_) {
            p0->Swap(px);
        }
    }

    split_req->set_allocated_new_range(range);

    auto ret = Submit(cmd);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("AdminSplit raft submit error: %s", ret.ToString().c_str());
    }
}

Status Range::ApplySplit(const raft_cmdpb::Command &cmd, uint64_t index) {
    RANGE_LOG_INFO("ApplySplit Begin, version: %" PRIu64 ", index: %" PRIu64, meta_.GetVersion(), index);

    const auto& req = cmd.admin_split_req();
    auto ret = meta_.CheckSplit(req.split_key(), cmd.verify_epoch().version());
    if (ret.code() == Status::kStaleEpoch) {
        RANGE_LOG_WARN("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                req.new_range().id(), ret.ToString().c_str());
        return Status::OK();
    } else if (ret.code() == Status::kOutOfBound) {
        // invalid split key, ignore split request
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                       req.new_range().id(), ret.ToString().c_str());
        return Status::OK();
    } else if (!ret.ok()) {
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                       req.new_range().id(), ret.ToString().c_str());
        return ret;
    }

    context_->run_status->IncrSplitCount();

    ret = context_->range_server->ApplySplit(id_, req, index);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") create failed: %s",
                        req.new_range().id(), ret.ToString().c_str());
        return ret;
    }

    meta_.Split(req.split_key(), req.epoch().version());
    store_->SetEndKey(req.split_key());

    if (req.leader() == node_id_) {
        ReportSplit(req.new_range());

        // new range report heartbeat
        split_range_id_ = req.new_range().id();
        context_->range_server->LeaderQueuePush(split_range_id_,
                                                getticks());

        uint64_t rsize = ds_config.range_config.split_size >> 1;
        auto rng = context_->range_server->find(split_range_id_);
        if (rng != nullptr) {
            rng->set_real_size(real_size_ - rsize);
        }

        real_size_ = rsize;

        // specify leader don't trigger function OnLeaderChange
        context_->run_status->IncrLeaderCount();
    }

    context_->run_status->DecrSplitCount();

    RANGE_LOG_INFO("ApplySplit(new range: %" PRIu64 ") End. version:%" PRIu64,
            req.new_range().id(), meta_.GetVersion());

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
