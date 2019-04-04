//
// Created by guo on 2/8/18.
//
#include "range.h"
#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

static const int64_t kDefaultKVMaxCount = 1000;

static int64_t checkMaxCount(int64_t maxCount) {
    if (maxCount <= 0)
        maxCount = std::numeric_limits<int64_t>::max();
    if (maxCount > kDefaultKVMaxCount) {
        maxCount = kDefaultKVMaxCount ;
    }
    return maxCount;
}

void Range::KVSet(RPCRequestPtr rpc, kvrpcpb::DsKvSetRequest &req) {
    auto &key = req.req().kv().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }

        if (!VerifyLeader(err)) {
            RANGE_LOG_WARN("KVSet error: %s", err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            RANGE_LOG_WARN("KVSet error: %s", err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("KvSet error: %s", err->message().c_str());
            break;
        }

        SubmitCmd<kvrpcpb::DsKvSetResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::KvSet);
                cmd.set_allocated_kv_set_req(req.release_req());
            });
    } while (false);

    if (err != nullptr) {
        kvrpcpb::DsKvSetResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyKVSet(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;
    uint64_t affected_keys = 0;

    //auto &req = cmd.kv_set_req();
    auto btime = NowMicros();
    ret = RangeBase::ApplyKVSet(cmd, &affected_keys, err);

//    do {
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)) {
//            RANGE_LOG_WARN("ApplyInsert error: %s", err->message().c_str());
//            break;
//        }
//
//        if (req.case_() != kvrpcpb::EC_Force) {
//            bool bExists = store_->KeyExists(req.kv().key());
//            if ((req.case_() == kvrpcpb::EC_Exists && !bExists) ||
//                (req.case_() == kvrpcpb::EC_NotExists && bExists)) {
//                break;
//            }
//            if (bExists) {
//                affected_keys = 1;
//            }
//        }
//        ret = store_->Put(req.kv().key(), req.kv().value());
//        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
//
//        if (cmd.cmd_id().node_id() == node_id_) {
//            auto len = req.kv().key().size() + req.kv().value().size();
//            CheckSplit(len);
//        }
//    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsInsertResponse resp;
        resp.mutable_resp()->set_affected_keys(affected_keys);
        resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVGet(RPCRequestPtr rpc, kvrpcpb::DsKvGetRequest &req) {
    errorpb::Error *err = nullptr;
    kvrpcpb::DsKvGetResponse ds_resp;

    RANGE_LOG_DEBUG("KVGet begin");
    do {
        auto &key = req.req().key();
        if (!VerifyLeader(err)) {
            RANGE_LOG_WARN("KVGet error: %s", err->message().c_str());
            break;
        }
        if (key.empty() || !KeyInRange(key, err)) {
            RANGE_LOG_WARN("KVGet error: %s", err->message().c_str());
            break;
        }

        auto resp = ds_resp.mutable_resp();
        auto btime = NowMicros();
        auto ret = store_->Get(req.req().key(), resp->mutable_value());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);

        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    SendResponse(rpc, ds_resp, req.header(), err);
}

void Range::KVBatchSet(RPCRequestPtr rpc, kvrpcpb::DsKvBatchSetRequest &req) {
    Status ret;
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyWriteable(&err)) {
            break;
        }

        if (!VerifyLeader(err)) {
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            break;
        }

        SubmitCmd<kvrpcpb::DsKvBatchSetResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::KvBatchSet);
                cmd.set_allocated_kv_batch_set_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("KVBatchSet error: %s", err->message().c_str());
        kvrpcpb::DsKvBatchSetResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyKVBatchSet(const raft_cmdpb::Command &cmd) {
    Status ret;

    auto total_size = 0, total_count = 0;
    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;
    auto btime = NowMicros();
    ret = RangeBase::ApplyKVBatchSet(cmd, affected_keys, err);

//    do {
//        auto &req = cmd.kv_batch_set_req();
//        auto existCase = req.case_();
//
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)) {
//            RANGE_LOG_WARN("ApplyKVBatchSet error: %s", err->message().c_str());
//            break;
//        }
//
//        std::vector<std::pair<std::string, std::string>> keyValues;
//        for (int i = 0, count = req.kvs_size(); i < count; ++i) {
//            auto kv = req.kvs(i);
//            do {
//                if (req.case_() != kvrpcpb::EC_Force) {
//                    bool bExists = store_->KeyExists(kv.key());
//                    if ((existCase == kvrpcpb::EC_Exists && !bExists) ||
//                        (existCase == kvrpcpb::EC_NotExists && bExists)) {
//                        break;
//                    }
//                    if (bExists) {
//                        ++affected_keys;
//                    }
//                }
//                total_size += kv.key().size() + kv.value().size();
//                ++total_count;
//
//                keyValues.push_back(std::pair<std::string, std::string>(kv.key(), kv.value()));
//            } while (false);
//        }
//
//        ret = store_->BatchSet(keyValues);
//        context_->Statistics()->PushTime(HistogramType::kStore,
//                                       NowMicros() - btime);
//
//        if (!ret.ok()) {
//            RANGE_LOG_ERROR("ApplyKVBatchSet failed, code:%d, msg:%s", ret.code(),
//                       ret.ToString().c_str());
//            break;
//        }
//
//        if (cmd.cmd_id().node_id() == node_id_) {
//            CheckSplit(total_size);
//        }
//    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvBatchSetResponse resp;
        resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
        resp.mutable_resp()->set_affected_keys(affected_keys);
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

void Range::KVBatchGet(RPCRequestPtr rpc, kvrpcpb::DsKvBatchGetRequest &req) {
    errorpb::Error *err = nullptr;
    kvrpcpb::DsKvBatchGetResponse ds_resp;
    auto header = ds_resp.mutable_header();
    auto total_time = 0L;

    uint64_t count = 0;
    uint64_t total_size = 0;
    auto keys_size = req.req().keys_size();

    for (int i = 0; i < keys_size; ++i) {
        auto &key = req.req().keys(i);
        if (key.empty() || !KeyInRange(key)) {
            RANGE_LOG_WARN("KVBatchGet error: %s not in range", key.c_str());
        } else {
            auto kv = ds_resp.mutable_resp()->add_kvs();
            auto btime = NowMicros();
            auto ret = store_->Get(key, kv->mutable_value());
            kv->set_key(key);
            count++;
            total_size += kv->key().size() + kv->value().size();
            total_time += NowMicros() - btime;
        }
    }
    context_->Statistics()->PushTime(HistogramType::kStore, total_time);

    SendResponse(rpc, ds_resp, req.header(), err);
}

void Range::KVDelete(RPCRequestPtr rpc, kvrpcpb::DsKvDeleteRequest &req) {
    auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyWriteable(&err)) {
            break;
        }
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(key, err)) {
            break;
        }

        SubmitCmd<kvrpcpb::DsKvDeleteResponse>(std::move(rpc), req.header(),
            [&req](raft_cmdpb::Command &cmd) {
                cmd.set_cmd_type(raft_cmdpb::CmdType::KvDelete);
                cmd.set_allocated_kv_delete_req(req.release_req());
        });
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("KVDelete error: %s", err->message().c_str());

        kvrpcpb::DsKvDeleteResponse resp;
        SendResponse(rpc, resp, req.header(), err);
    }
}

Status Range::ApplyKVDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;
    auto btime = NowMicros();
    ret = RangeBase::ApplyKVDelete(cmd, err);
//    do {
//        auto &req = cmd.kv_delete_req();
//
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)) {
//            RANGE_LOG_WARN("ApplyKVBatchSet error: %s", err->message().c_str());
//            break;
//        }
//
//        ret = store_->Delete(req.key());
//        context_->Statistics()->PushTime(HistogramType::kStore,
//                                       NowMicros() - btime);
//
//        if (!ret.ok()) {
//            RANGE_LOG_ERROR("ApplyKVDelete failed, code:%d, msg:%s", ret.code(),
//                       ret.ToString().c_str());
//            break;
//        }
//    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvDeleteResponse resp;
        resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVBatchDelete(RPCRequestPtr rpc, kvrpcpb::DsKvBatchDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    if (!VerifyLeader(err) || !VerifyWriteable(&err)) {
        RANGE_LOG_WARN("Insert error: %s", err->message().c_str());
        kvrpcpb::DsKvBatchDeleteResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        RANGE_LOG_WARN("Insert error: %s", err->message().c_str());

        kvrpcpb::DsKvBatchDeleteResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    for (int i = 0, count = req.req().keys_size(); i < count; ++i) {
        auto &key = req.req().keys(i);
        if (!KeyInRange(key)) {
            kvrpcpb::DsKvBatchDeleteResponse resp;
            return SendResponse(rpc, resp, req.header(), KeyNotInRange(key));
        }
    }

    SubmitCmd<kvrpcpb::DsKvBatchDeleteResponse>(std::move(rpc), req.header(),
        [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvBatchDel);
            cmd.set_allocated_kv_batch_del_req(req.release_req());
        });
}

Status Range::ApplyKVBatchDelete(const raft_cmdpb::Command &cmd) {
    Status ret;

    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;
    auto btime = NowMicros();
    ret = RangeBase::ApplyKVBatchDelete(cmd, affected_keys, err);
//    do {
//        auto &req = cmd.kv_batch_del_req();
//        std::vector<std::string> delKeys(req.keys_size());
//
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)) {
//            RANGE_LOG_WARN("ApplyKVBatchDelete error: %s", err->message().c_str());
//            break;
//        }
//
//        for (int i = 0, count = req.keys_size(); i < count; ++i) {
//            auto &key = req.keys(i);
//            if (req.case_() == kvrpcpb::EC_Exists ||
//                req.case_() == kvrpcpb::EC_AnyCase) {
//                if (store_->KeyExists(key)) {
//                    ++affected_keys;
//                    delKeys.push_back(std::move(key));
//                }
//            } else {
//                delKeys.push_back(std::move(key));
//            }
//        }
//
//        ret = store_->BatchDelete(delKeys);
//        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
//
//        if (!ret.ok()) {
//            RANGE_LOG_ERROR("ApplyKVBatchDelete failed, code:%d, msg:%s", ret.code(),
//                       ret.ToString().c_str());
//            break;
//        }
//
//    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvBatchDeleteResponse resp;
        resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
        resp.mutable_resp()->set_affected_keys(affected_keys);
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVRangeDelete(RPCRequestPtr rpc, kvrpcpb::DsKvRangeDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    if (!VerifyLeader(err) || !VerifyWriteable(&err)) {
        RANGE_LOG_WARN("KVRangeDelet error: %s", err->message().c_str());

        kvrpcpb::DsKvRangeDeleteResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        RANGE_LOG_WARN("KVRangeDelet error: %s", err->message().c_str());

        kvrpcpb::DsKvRangeDeleteResponse resp;
        return SendResponse(rpc, resp, req.header(), err);
    }

    SubmitCmd<kvrpcpb::DsKvRangeDeleteResponse>(std::move(rpc), req.header(),
        [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvRangeDel);
            cmd.set_allocated_kv_range_del_req(req.release_req());
    });
}

Status Range::ApplyKVRangeDelete(const raft_cmdpb::Command &cmd) {
    Status ret;

    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;
    std::string last_key;
    auto btime = NowMicros();
    ret = RangeBase::ApplyKVRangeDelete(cmd, affected_keys, err);
//    do {
//        auto &req = cmd.kv_range_del_req();
//        auto start = std::max(req.start(), start_key_);
//        auto limit = std::min(req.limit(), meta_.GetEndKey());
//
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)) {
//            RANGE_LOG_WARN("KVRangeDelet error: %s", err->message().c_str());
//            break;
//        }
//
//        if (req.case_() == kvrpcpb::EC_Exists ||
//            req.case_() == kvrpcpb::EC_AnyCase) {
//            std::unique_ptr<storage::IteratorInterface> iterator(
//                store_->NewIterator(start, limit));
//            int maxCount = checkMaxCount(req.max_count());
//            std::vector<std::string> delKeys(maxCount);
//
//            for (int i = 0; iterator->Valid() && i < maxCount; ++i) {
//                delKeys.push_back(std::move(iterator->key()));
//                iterator->Next();
//            }
//
//            if (delKeys.size() > 0) {
//                last_key = delKeys[delKeys.size() - 1];
//            }
//
//            ret = store_->BatchDelete(delKeys);
//            affected_keys = delKeys.size();
//        } else {
//            ret = store_->RangeDelete(start, limit);
//        }
//
//        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
//    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        kvrpcpb::DsKvRangeDeleteResponse resp;
        resp.mutable_resp()->set_last_key(last_key);
        resp.mutable_resp()->set_affected_keys(affected_keys);
        resp.mutable_resp()->set_code(static_cast<int32_t>(ret.code()));
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

void Range::KVScan(RPCRequestPtr rpc, kvrpcpb::DsKvScanRequest &req) {
    errorpb::Error *err = nullptr;
    kvrpcpb::DsKvScanResponse ds_resp;
    auto start = std::max(req.req().start(), start_key_);
    auto limit = std::min(req.req().limit(), meta_.GetEndKey());
    std::unique_ptr<storage::IteratorInterface> iterator(
        store_->NewIterator(start, limit));

    int max_count = checkMaxCount(req.req().max_count());
    auto resp = ds_resp.mutable_resp();

    uint64_t count = 0;
    uint64_t total_size = 0;

    for (int i = 0; iterator->Valid() && i < max_count; ++i) {
        auto kv = resp->add_kvs();
        kv->set_key(std::move(iterator->key()));
        kv->set_value(std::move(iterator->value()));
        iterator->Next();

        count++;
        total_size += kv->key().length() + kv->value().length();
    }

    if (resp->kvs_size() > 0) {
        resp->set_last_key(resp->kvs(resp->kvs_size() - 1).key());
    }

    SendResponse(rpc, ds_resp, req.header(), err);
}

}
}
}  // for namespace
