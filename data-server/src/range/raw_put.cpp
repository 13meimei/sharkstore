#include "range.h"

#include "server/range_server.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::RawPut(RPCRequestPtr rpc, kvrpcpb::DsKvRawPutRequest &req, bool redirect) {
    RANGE_LOG_DEBUG("RawPut begin");

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
            RANGE_LOG_WARN("RawPut error: key empty");
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
                rng->RawPut(std::move(rpc), req, false);
                return;
            }
        }

        SubmitCmd(std::move(rpc), req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::RawPut);
            cmd.set_allocated_kv_raw_put_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("RawPut error: %s", err->message().c_str());

        kvrpcpb::DsKvRawPutResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyRawPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    RANGE_LOG_DEBUG("ApplyRawPut begin");
    auto &req = cmd.kv_raw_put_req();
    auto btime = NowMicros();

    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("Apply RawPut failed, epoch is changed");
            ret = Status(Status::kInvalidArgument, "key not int range", "");
            break;
        }

        ret = store_->Put(req.key(), req.value());
        context_->Statistics()->PushTime(HistogramType::kStore,
                                       NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyRawPut failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().size();
            CheckSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvRawPutResponse resp;
        if (!ret.ok()) {
            resp.mutable_resp()->set_code(static_cast<int32_t>(ret.code()));
        }
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
