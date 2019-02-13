#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

static void setTxnServerErr(txnpb::TxnError* err, int32_t code, const std::string& msg) {
    err->set_err_type(txnpb::TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
}

bool Range::KeyInRange(const txnpb::PrepareRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
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

bool Range::KeyInRange(const txnpb::DecideRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
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

void Range::TxnPrepare(common::ProtoMessage* msg, txnpb::DsPrepareRequest& req) {
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
        auto resp = new txnpb::DsPrepareResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyTxnPrepare(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnPrepare begin");

    Status ret;
    auto &req = cmd.txn_prepare_req();
    auto btime = get_micro_second();
    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        // TODO:
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new txnpb::DsPrepareResponse;
        if (!ret.ok()) {
            setTxnServerErr(resp->mutable_resp()->add_errors(), ret.code(), ret.ToString());
        }
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnDecide(common::ProtoMessage* msg, txnpb::DsDecideRequest& req) {
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
        auto resp = new txnpb::DsPrepareResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyTxnDecide(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnDecide begin");

    Status ret;
    auto &req = cmd.txn_decide_req();
    auto btime = get_micro_second();
    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        // TODO:
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new txnpb::DsDecideResponse;
        if (!ret.ok()) {
            setTxnServerErr(resp->mutable_resp()->mutable_err(), ret.code(), ret.ToString());
        }
        ReplySubmit(cmd, resp, err, btime);
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

    do {
        if (!EpochIsEqual(cmd.verify_epoch(), err)) {
            break;
        }
        if (!KeyInRange(req.primary_key(), err)) {
            break;
        }
        // TODO:
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new txnpb::DsClearupResponse;
        if (!ret.ok()) {
            setTxnServerErr(resp->mutable_resp()->mutable_err(), ret.code(), ret.ToString());
        }
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnGetLockInfo(common::ProtoMessage* msg, txnpb::DsGetLockInfoRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);
}

void Range::TxnSelect(common::ProtoMessage* msg, txnpb::DsSelectRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
