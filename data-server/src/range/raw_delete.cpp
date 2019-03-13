#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::RawDelete(RPCRequestPtr rpc, kvrpcpb::DsKvRawDeleteRequest &req) {
    rawDelete(std::move(rpc), req, true);
}

void Range::rawDelete(RPCRequestPtr rpc, kvrpcpb::DsKvRawDeleteRequest &req, bool redirect) {
    RANGE_LOG_DEBUG("RawDelete begin");

    errorpb::Error *err = nullptr;
    do {
        if (!VerifyWriteable(&err)) {
            break;
        }

        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();

        if (key.empty()) {
            RANGE_LOG_WARN("RawDelete error: key empty");
            err = KeyNotInRange(key);
            break;
        }

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

            // 单key: 不在范围内, 尝试转发给分裂出去的range, 减少网络交互
            auto rng = context_->FindRange(split_range_id_);
            if (rng == nullptr || !redirect) {
                err = StaleEpochError(epoch);
                break;
            } else {
                // 重定向, 只重定向一次
                rng->rawDelete(std::move(rpc), req, false);
                return;
            }
        }

        SubmitCmd<kvrpcpb::DsKvRawDeleteResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::RawDelete);
                cmd.set_allocated_kv_raw_delete_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("RawDelete error: %s", err->message().c_str());

        kvrpcpb::DsKvRawDeleteResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyRawDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyRawDelete begin");

    auto btime = NowMicros();
    auto &req = cmd.kv_raw_delete_req();

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("ApplyRawDelete failed, epoch is changed");
            break;
        }

        ret = store_->Delete(req.key());
        context_->Statistics()->PushTime(HistogramType::kStore,
                                       NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyRawDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvRawDeleteResponse resp;
        resp.mutable_resp()->set_code(ret.code());
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
