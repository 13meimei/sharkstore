#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;
using namespace txnpb;


bool Range::KeyInRange(const PrepareRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
    assert(err != nullptr);
    bool in_range = true;
    std::string wrong_key;
    for (const auto& intent: req.intents()) {
        if (!KeyInRange(intent.key())) {
            in_range = false;
            wrong_key = intent.key();
            break;
        }
    }
    if (in_range) return true;
    // not in range
    if (!EpochIsEqual(epoch)) {
        *err = StaleEpochError(epoch);
    } else {
        *err = KeyNotInRange(wrong_key);
    }
    return false;
}

bool Range::KeyInRange(const DecideRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
    assert(err != nullptr);
    bool in_range = true;
    std::string wrong_key;
    for (const auto& key: req.keys()) {
        if (!KeyInRange(key)) {
            in_range = false;
            wrong_key = key;
            break;
        }
    }
    if (in_range) return true;
    // not in range
    if (!EpochIsEqual(epoch)) {
        *err = StaleEpochError(epoch);
    } else {
        *err = KeyNotInRange(wrong_key);
    }
    return false;
}

void Range::TxnPrepare(common::ProtoMessage* msg, DsPrepareRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("TxnPrepare begin");

    errorpb::Error *err = nullptr;

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(req.req(), req.header().range_epoch(), &err)) {
            break;
        }

        // submit to raft
        // TODO: check lock in memory
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::TxnPrepare);
            cmd.set_allocated_txn_prepare_req(req.release_req());
        });
        if (!ret.ok()) {
            RANGE_LOG_WARN("TxnPrepare sumbit to raft failed: %s", ret.ToString().c_str());
            err = RaftFailError();
            break;
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnPrepare error: %s", err->message().c_str());
        auto resp = new DsPrepareResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyTxnPrepare(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnPrepare begin");

    Status ret;
    auto &req = cmd.txn_prepare_req();
    auto btime = get_micro_second();
    errorpb::Error *err = nullptr;
    std::unique_ptr<PrepareResponse> resp(new PrepareResponse);

    do {
        if (!KeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        store_->TxnPrepare(req, raft_index, resp.get());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto ds_resp = new DsPrepareResponse;
        ds_resp->set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnDecide(common::ProtoMessage* msg, DsDecideRequest& req) {
    RANGE_LOG_DEBUG("TxnDecide begin");

    errorpb::Error *err = nullptr;
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(req.req(), req.header().range_epoch(), &err)) {
            break;
        }
        // submit to raft
        // TODO: check lock in memory
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::TxnDecide);
            cmd.set_allocated_txn_decide_req(req.release_req());
        });
        if (!ret.ok()) {
            RANGE_LOG_WARN("TxnDecide sumbit to raft failed: %s", ret.ToString().c_str());
            err = RaftFailError();
            break;
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnDecide error: %s", err->message().c_str());
        auto resp = new DsPrepareResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyTxnDecide(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnDecide begin");

    Status ret;
    auto &req = cmd.txn_decide_req();
    auto btime = get_micro_second();
    errorpb::Error *err = nullptr;
    std::unique_ptr<DecideResponse> resp(new DecideResponse);
    uint64_t bytes_written = 0;

    do {
        if (!KeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        bytes_written = store_->TxnDecide(req, resp.get());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto ds_resp = new txnpb::DsDecideResponse;
        ds_resp->set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
        if (bytes_written > 0) {
            CheckSplit(bytes_written);
        }
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnClearup(common::ProtoMessage* msg, txnpb::DsClearupRequest& req) {
    RANGE_LOG_DEBUG("TxnClearup  begin");

    errorpb::Error *err = nullptr;
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!EpochIsEqual(req.header().range_epoch(), err)) {
            break;
        }
        if (!KeyInRange(req.req().primary_key(), err)) {
            break;
        }
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::TxnClearup);
            cmd.set_allocated_txn_clearup_req(req.release_req());
        });
        if (!ret.ok()) {
            RANGE_LOG_WARN("TxnClearup sumbit to raft failed: %s", ret.ToString().c_str());
            err = RaftFailError();
            break;
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnClearup error: %s", err->message().c_str());
        auto resp = new txnpb::DsClearupResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyTxnClearup(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    Status ret;
    auto &req = cmd.txn_clearup_req();
    auto btime = get_micro_second();
    errorpb::Error *err = nullptr;
    std::unique_ptr<ClearupResponse> resp(new ClearupResponse);

    do {
        if (!EpochIsEqual(cmd.verify_epoch(), err)) {
            break;
        }
        if (!KeyInRange(req.primary_key(), err)) {
            break;
        }
        store_->TxnClearup(req, resp.get());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto ds_resp = new txnpb::DsClearupResponse;
        ds_resp->set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnGetLockInfo(common::ProtoMessage* msg, txnpb::DsGetLockInfoRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new txnpb::DsGetLockInfoResponse;
    auto header = ds_resp->mutable_header();

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if (!EpochIsEqual(req.header().range_epoch(), err)) {
            break;
        }

        if (!KeyInRange(req.req().key(), err)) {
            break;
        }

        auto now = get_micro_second();
        store_->TxnGetLockInfo(req.req(), ds_resp->mutable_resp());
        context_->Statistics()->PushTime(HistogramType::kStore, now - btime);
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_ERROR("TxnGetLockInfo failed: %s", err->ShortDebugString().c_str());
    }

    common::SetResponseHeader(req.header(), header, err);
    context_->SocketSession()->Send(msg, ds_resp);
}

void Range::TxnSelect(common::ProtoMessage* msg, txnpb::DsSelectRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
