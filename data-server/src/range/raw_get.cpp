#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::RawGet(RPCRequestPtr rpc, kvrpcpb::DsKvRawGetRequest &req, bool redirect) {
    RANGE_LOG_DEBUG("RawGet begin");

    errorpb::Error *err = nullptr;
    kvrpcpb::DsKvRawGetResponse ds_resp;
    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        if (key.empty()) {
            RANGE_LOG_WARN("RawGet error: key empty");
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
                rng->RawGet(std::move(rpc), req, false);
                return;
            }
        }

        auto resp = ds_resp.mutable_resp();
        auto btime = NowMicros();
        auto ret = store_->Get(req.req().key(), resp->mutable_value());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("RawGet error: %s", err->message().c_str());
    }

    SendResponse(rpc, ds_resp, req.header(), err);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
