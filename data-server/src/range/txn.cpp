#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;
using namespace txnpb;


bool Range::TxnKeyInRange(const PrepareRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
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

bool Range::TxnKeyInRange(const DecideRequest& req, const metapb::RangeEpoch& epoch, errorpb::Error** err) {
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

void Range::TxnPrepare(RPCRequestPtr rpc, DsPrepareRequest& req) {
    RANGE_LOG_DEBUG("TxnPrepare begin, req: %s", req.DebugString().c_str());

    errorpb::Error *err = nullptr;
    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!TxnKeyInRange(req.req(), req.header().range_epoch(), &err)) {
            break;
        }

        // submit to raft
        // TODO: check lock in memory
        SubmitCmd<DsPrepareResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::TxnPrepare);
                cmd.set_allocated_txn_prepare_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnPrepare error: %s", err->message().c_str());
        DsPrepareResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyTxnPrepare(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnPrepare begin at %" PRId64 ", req: %s", raft_index, cmd.DebugString().c_str());

    Status ret;
    auto &req = cmd.txn_prepare_req();
    auto btime = NowMicros();
    errorpb::Error *err = nullptr;
    std::unique_ptr<PrepareResponse> resp(new PrepareResponse);
    do {
        if (!TxnKeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        store_->TxnPrepare(req, raft_index, resp.get());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        DsPrepareResponse ds_resp;
        ds_resp.set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnDecide(RPCRequestPtr rpc, DsDecideRequest& req) {
    RANGE_LOG_DEBUG("TxnDecide begin, req: %s", req.DebugString().c_str());

    errorpb::Error *err = nullptr;
    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!TxnKeyInRange(req.req(), req.header().range_epoch(), &err)) {
            break;
        }

        // submit to raft
        // TODO: check lock in memory
        SubmitCmd<DsDecideResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::TxnDecide);
                cmd.set_allocated_txn_decide_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnDecide error: %s", err->message().c_str());
        DsDecideResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyTxnDecide(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    RANGE_LOG_DEBUG("ApplyTxnDecide begin, cmd: %s", cmd.DebugString().c_str());

    Status ret;
    auto &req = cmd.txn_decide_req();
    auto btime = NowMicros();
    errorpb::Error *err = nullptr;
    std::unique_ptr<DecideResponse> resp(new DecideResponse);
    uint64_t bytes_written = 0;

    do {
        if (!TxnKeyInRange(req, cmd.verify_epoch(), &err)) {
            break;
        }
        bytes_written = store_->TxnDecide(req, resp.get());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        txnpb::DsDecideResponse ds_resp;
        ds_resp.set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
        if (bytes_written > 0) {
            CheckSplit(bytes_written);
        }
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnClearup(RPCRequestPtr rpc, txnpb::DsClearupRequest& req) {
    RANGE_LOG_DEBUG("TxnClearup  begin, req: %s", req.DebugString().c_str());

    errorpb::Error *err = nullptr;
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

        SubmitCmd<DsClearupResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::TxnClearup);
                cmd.set_allocated_txn_clearup_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnClearup error: %s", err->message().c_str());

        txnpb::DsClearupResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyTxnClearup(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    Status ret;
    auto &req = cmd.txn_clearup_req();
    auto btime = NowMicros();
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

    if (err != nullptr) {
        RANGE_LOG_ERROR("TxnClearup %s failed: %s", req.primary_key().c_str(),
                        err->ShortDebugString().c_str());
    } else if (resp->has_err()) {
        RANGE_LOG_ERROR("TxnClearup %s failed: %s", req.primary_key().c_str(),
                resp->err().ShortDebugString().c_str());
    }

    if (cmd.cmd_id().node_id() == node_id_) {
        txnpb::DsClearupResponse ds_resp;
        ds_resp.set_allocated_resp(resp.release());
        ReplySubmit(cmd, ds_resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::TxnGetLockInfo(RPCRequestPtr rpc, txnpb::DsGetLockInfoRequest& req) {
    auto btime = NowMicros();
    errorpb::Error *err = nullptr;
    txnpb::DsGetLockInfoResponse ds_resp;

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

        auto now = NowMicros();
        store_->TxnGetLockInfo(req.req(), ds_resp.mutable_resp());
        context_->Statistics()->PushTime(HistogramType::kStore, now - btime);
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_ERROR("TxnGetLockInfo failed: %s", err->ShortDebugString().c_str());
    }

    SendResponse(rpc, ds_resp, req.header(), err);
}

void Range::TxnSelect(RPCRequestPtr rpc, txnpb::DsSelectRequest& req) {
    RANGE_LOG_DEBUG("Select begin, req: %s", req.DebugString().c_str());

    auto btime = NowMicros();
    errorpb::Error *err = nullptr;
   txnpb::DsSelectResponse ds_resp;
    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if (!EpochIsEqual(req.header().range_epoch(), err)) {
            break;
        }

        auto key = req.req().key();
        if (!key.empty() && !KeyInRange(key, err)) {
            break;
        }

        auto resp = ds_resp.mutable_resp();
        auto ret = store_->TxnSelect(req.req(), resp);
        auto etime = NowMicros();
        context_->Statistics()->PushTime(HistogramType::kStore, etime - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("TxnSelect from store error: %s", ret.ToString().c_str());
            resp->set_code(static_cast<int>(ret.code()));
            break;
        }

        if (key.empty() && !EpochIsEqual(req.header().range_epoch(), err)) {
            ds_resp.clear_resp();
            RANGE_LOG_WARN("epoch change Select error: %s", err->message().c_str());
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("TxnSelect error: %s", err->message().c_str());
    } else {
        RANGE_LOG_DEBUG("TxnSelect result: code=%d, rows=%d",
                        (ds_resp.has_resp() ? ds_resp.resp().code() : 0),
                        (ds_resp.has_resp() ? ds_resp.resp().rows_size() : 0));
    }

    SendResponse(rpc, ds_resp, req.header(), err);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
