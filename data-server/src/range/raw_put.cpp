#include "range.h"

#include "server/range_server.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

bool Range::RawPutSubmit(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && KeyInRange(key)) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::RawPut);
            cmd.set_allocated_kv_raw_put_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::RawPutTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req) {
    auto rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    return rng->RawPutSubmit(msg, req);
}

void Range::RawPut(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("RawPut begin");

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvRawPutResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
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

            //! is_equal then retry raw put
            if (!RawPutTry(msg, req)) {
                err = StaleEpochError(epoch);
            }

            break;
        }

        if (!RawPutSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("RawPut error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsKvRawPutResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyRawPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    RANGE_LOG_DEBUG("ApplyRawPut begin");
    auto &req = cmd.kv_raw_put_req();

    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("Apply RawPut failed, epoch is changed");
            ret = std::move(Status(Status::kInvalidArgument, "key not int range", ""));
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Put(req.key(), req.value());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

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
        auto resp = new kvrpcpb::DsKvRawPutResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
