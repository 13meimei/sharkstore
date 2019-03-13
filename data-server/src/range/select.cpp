#include "range.h"

#include "frame/sf_logger.h"
#include "server/range_server.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::Select(RPCRequestPtr rpc, kvrpcpb::DsSelectRequest &req) {
    select(std::move(rpc), req, true);
}

void Range::select(RPCRequestPtr rpc, kvrpcpb::DsSelectRequest &req, bool redirect) {
    RANGE_LOG_DEBUG("Select begin");

    kvrpcpb::DsSelectResponse ds_resp;
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyReadable(req.header().read_index(), err)) {
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
            if (!KeyInRange(key)) {
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
                    rng->select(std::move(rpc), req, false);
                    return;
                }
            }
        }

        auto resp = ds_resp.mutable_resp();
        auto btime = NowMicros();
        auto ret = store_->Select(req.req(), resp);
        auto etime = NowMicros();
        context_->Statistics()->PushTime(HistogramType::kStore, etime - btime);

        if (etime - rpc->begin_time > kTimeTakeWarnThresoldUSec) {
            RANGE_LOG_WARN("select takes too long(%" PRId64 " ms), from %s, msgid=%" PRId64,
                      (etime - rpc->begin_time) / 1000, rpc->ctx.remote_addr.c_str(),
                      rpc->msg->head.msg_id);
        }

        if (ret.ok()) {
            resp->set_code(0);
        } else {
            RANGE_LOG_WARN("Select from store error: %s", ret.ToString().c_str());

            resp->set_code(static_cast<int>(ret.code()));
        }

        if (key.empty() && !EpochIsEqual(epoch, err)) {
            ds_resp.clear_resp();
            RANGE_LOG_WARN("epoch change Select error: %s", err->message().c_str());
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("Select error: %s", err->message().c_str());
    }

    RANGE_LOG_DEBUG("Select result: code=%d, rows=%d",
            (ds_resp.has_resp() ? ds_resp.resp().code() : 0),
            (ds_resp.has_resp() ? ds_resp.resp().rows_size() : 0));

    ds_resp.mutable_header()->set_apply_index(apply_index_);
    SendResponse(rpc, ds_resp, req.header(), err);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
