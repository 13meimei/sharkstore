#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::Delete(RPCRequestPtr rpc, kvrpcpb::DsDeleteRequest &req, bool redirect) {
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("Delete begin");

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }

        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        auto epoch = req.header().range_epoch();

        bool is_equal = EpochIsEqual(epoch);

        if (key.empty()) {
            if (!is_equal) {
                err = StaleEpochError(epoch);
                break;
            }
        } else {
            bool in_range = KeyInRange(key);
            if (!in_range) {
                // 如果epoch相等但是key却不在范围，则一定是个错误
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
                    rng->Delete(std::move(rpc), req, false);
                    return;
                }
            }
        }

        SubmitCmd<kvrpcpb::DsKvDeleteResponse>(std::move(rpc), req.header(),
             [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::Delete);
                cmd.set_allocated_delete_req(req.release_req());
             }
        );
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("Delete error: %s", err->message().c_str());

        kvrpcpb::DsKvDeleteResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyDelete begin");

    auto &req = cmd.delete_req();
    auto btime = NowMicros();

    do {
        auto &key = req.key();
        if (key.empty()) {
            auto &epoch = cmd.verify_epoch();

            if (!EpochIsEqual(epoch, err)) {
                RANGE_LOG_WARN("ApplyDelete error: %s", err->message().c_str());
                break;
            }
        } else {
            if (!KeyInRange(key, err)) {
                RANGE_LOG_WARN("ApplyDelete error: %s", err->message().c_str());
                break;
            }
        }

        ret = store_->DeleteRows(req, &affected_keys);
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvDeleteResponse resp;
        resp.mutable_resp()->set_affected_keys(affected_keys);
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
