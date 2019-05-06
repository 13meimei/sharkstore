#include "range.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::Update(RPCRequestPtr rpc, kvrpcpb::DsUpdateRequest &req) {
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("Update begin");

    if (!VerifyLeader(err) || !hasSpaceLeft(&err)) {
        RANGE_LOG_WARN("Update error: %s", err->message().c_str());

        kvrpcpb::DsUpdateResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        RANGE_LOG_WARN("Update error: %s", err->message().c_str());

        kvrpcpb::DsUpdateResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    SubmitCmd<kvrpcpb::DsUpdateResponse>(std::move(rpc), req.header(),
        [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Update);
            cmd.set_allocated_update_req(req.release_req());
    });
}

Status Range::ApplyUpdate(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;

    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyUpdate begin");

    auto &req = cmd.update_req();
    auto btime = NowMicros();
    do {
        auto &epoch = cmd.verify_epoch();

        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyUpdate error: %s", err->message().c_str());
            break;
        }

        uint64_t update_bytes = 0;

        ret = store_->Update(req, &affected_keys, &update_bytes);
        auto etime = NowMicros();
        context_->Statistics()->PushTime(HistogramType::kStore, etime - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyUpdate failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            CheckSplit(update_bytes);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsUpdateResponse resp;
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