#include "range_base.h"

#include <cinttypes>
#include "storage/meta_store.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

// 磁盘使用率大于百分之92停写
static const uint64_t kStopWriteFsUsagePercent = 92;

int64_t checkMaxCount(int64_t maxCount) {
    if (maxCount <= 0)
        maxCount = std::numeric_limits<int64_t>::max();
    if (maxCount > kDefaultKVMaxCount) {
        maxCount = kDefaultKVMaxCount ;
    }
    return maxCount;
}

RangeBase::RangeBase(RangeContext* context, const metapb::Range &meta) :
        context_(context),
        node_id_(context_->GetNodeID()),
        id_(meta.id()),
        start_key_(meta.start_key()),
        meta_(meta)
{
    //store_ = std::unique_ptr<storage::Store>(new storage::Store(meta, context->DBInstance()));
}

RangeBase::~RangeBase() {}

Status RangeBase::Initialize(uint64_t leader, uint64_t log_start_index, uint64_t sflag) {
    slave_flag_ = sflag;
    store_ = std::unique_ptr<storage::Store>(new storage::Store(std::move(meta_.Get()), context_->DBInstance(slave_flag_)));

    return Status::OK();
}

Status RangeBase::Shutdown() {
    valid_ = false;
    //just master range do this?
    //context_->Statistics()->ReportLeader(id_, false);
    return Status::OK();
}

Status RangeBase::Apply(const raft_cmdpb::Command &cmd, uint64_t index) {
    if (slave_flag_ == 0 && !VerifyWriteable()) {
        return Status(Status::kIOError, "no left space", "apply");
    }

    Status sts;
    switch (cmd.cmd_type()) {
//        case raft_cmdpb::CmdType::Lock:
//            return ApplyLock(cmd, index);
//        case raft_cmdpb::CmdType::LockUpdate:
//            return ApplyLockUpdate(cmd);
//        case raft_cmdpb::CmdType::Unlock:
//            return ApplyUnlock(cmd);
//        case raft_cmdpb::CmdType::UnlockForce:
//            return ApplyUnlockForce(cmd);
//        case raft_cmdpb::CmdType::KvWatchPut:
//            return ApplyWatchPut(cmd, index);
//        case raft_cmdpb::CmdType::KvWatchDel:
//            return ApplyWatchDel(cmd, index);

        case raft_cmdpb::CmdType::RawPut:
            sts = ApplyRawPut(cmd);
            break;
        case raft_cmdpb::CmdType::RawDelete:
            sts = ApplyRawDelete(cmd);
            break;
        case raft_cmdpb::CmdType::Insert:
            sts = ApplyInsert(cmd);
            break;
        case raft_cmdpb::CmdType::Update:
            sts = ApplyUpdate(cmd);
            break;
        case raft_cmdpb::CmdType::Delete:
            sts = ApplyDelete(cmd);
            break;
        case raft_cmdpb::CmdType::KvSet:
            sts = ApplyKVSet(cmd);
            break;
        case raft_cmdpb::CmdType::KvBatchSet:
            sts = ApplyKVBatchSet(cmd);
            break;
        case raft_cmdpb::CmdType::KvDelete:
            sts = ApplyKVDelete(cmd);
            break;
        case raft_cmdpb::CmdType::KvBatchDel:
            sts = ApplyKVBatchDelete(cmd);
            break;
        case raft_cmdpb::CmdType::KvRangeDel:
            sts = ApplyKVRangeDelete(cmd);
            break;
//            case raft_cmdpb::CmdType::TxnPrepare:
//                return ApplyTxnPrepare(cmd, index);
//            case raft_cmdpb::CmdType::TxnDecide:
//                return ApplyTxnDecide(cmd, index);
//            case raft_cmdpb::CmdType::TxnClearup:
//                return ApplyTxnClearup(cmd, index);
        default:
            RANGE_LOG_ERROR("Apply cmd type error %s", CmdType_Name(cmd.cmd_type()).c_str());
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }

    return sts;
}

Status RangeBase::ApplyRawPut(const raft_cmdpb::Command &cmd, errorpb::Error *&err) {
    Status ret;

    RANGE_LOG_DEBUG("ApplyRawPut begin");
    auto &req = cmd.kv_raw_put_req();
    auto btime = NowMicros();

//    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("Apply RawPut failed, epoch is changed");
            ret = Status(Status::kInvalidArgument, "key not int range", "");
            break;
        }

        ret = store_->Put(req.key(), req.value());

        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyRawPut failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

        //only master range check
        if (slave_flag_ != 1 && cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().size();
            CheckSplit(len);
        }
    } while (false);

//    if (err != nullptr) {
//        delete err;
//    }

    return ret;
}

Status RangeBase::ApplyRawDelete(const raft_cmdpb::Command &cmd, errorpb::Error *&err) {
    Status ret;
//    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyRawDelete begin");

    auto btime = NowMicros();
    auto &req = cmd.kv_raw_delete_req();

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("ApplyRawDelete failed, epoch is changed");
            break;
        }

        ret = store_->Delete(req.key());
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyRawDelete failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

//    if (err != nullptr) {
//        delete err;
//    }
    return ret;
}

Status RangeBase::ApplyInsert(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;
//    uint64_t affected_keys = 0;
//    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyInsert begin");

    auto &req = cmd.insert_req();
    auto btime = NowMicros();
    do {
        auto &epoch = cmd.verify_epoch();

        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyInsert error: %s", err->message().c_str());
            break;
        }

        ret = store_->Insert(req, &affected_keys);
        auto etime = NowMicros();
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, etime - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyInsert failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

        if (slave_flag_ == 0 && cmd.cmd_id().node_id() == node_id_) {
            uint64_t len = 0;
            auto size = req.rows_size();

            for (int i = 0; i < size; i++) {
                auto keys_size = req.rows(i).key().size();
                auto values_size = req.rows(i).value().size();

                len += keys_size + values_size;
            }

            CheckSplit(len);
        }

    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsInsertResponse resp;
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            resp.mutable_resp()->set_code(ret.code());
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//        delete err;
//    }

    return ret;
}

Status RangeBase::ApplyUpdate(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;
//    uint64_t affected_keys = 0;
//    errorpb::Error *err = nullptr;

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
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, etime - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyUpdate failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

        if (slave_flag_ == 0 && cmd.cmd_id().node_id() == node_id_) {
            CheckSplit(update_bytes);
        }
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsUpdateResponse resp;
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            resp.mutable_resp()->set_code(ret.code());
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//        delete err;
//    }

    return ret;
}

Status RangeBase::ApplyDelete(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;
//    uint64_t affected_keys = 0;
//    errorpb::Error *err = nullptr;

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
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyDelete failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsKvDeleteResponse resp;
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            resp.mutable_resp()->set_code(ret.code());
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//        delete err;
//    }

    return ret;
}

Status RangeBase::ApplyKVSet(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;
    //errorpb::Error *err = nullptr;
    //uint64_t affected_keys = 0;

    auto &req = cmd.kv_set_req();
    auto btime = NowMicros();
    do {
        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyInsert error: %s", err->message().c_str());
            break;
        }

        if (req.case_() != kvrpcpb::EC_Force) {
            bool bExists = store_->KeyExists(req.kv().key());
            if ((req.case_() == kvrpcpb::EC_Exists && !bExists) ||
                (req.case_() == kvrpcpb::EC_NotExists && bExists)) {
                break;
            }
            if (bExists) {
                affected_keys = 1;
            }
        }
        ret = store_->Put(req.kv().key(), req.kv().value());
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, NowMicros() - btime);

        if (slave_flag_ == 0 && cmd.cmd_id().node_id() == node_id_) {
            auto len = req.kv().key().size() + req.kv().value().size();
            CheckSplit(len);
        }
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsInsertResponse resp;
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//            delete err;
//        }

    return ret;
}

Status RangeBase::ApplyKVBatchSet(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;

    auto total_size = 0, total_count = 0;
    //uint64_t affected_keys = 0;
    //errorpb::Error *err = nullptr;
    auto btime = NowMicros();

    do {
        auto &req = cmd.kv_batch_set_req();
        auto existCase = req.case_();

        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyKVBatchSet error: %s", err->message().c_str());
            break;
        }

        std::vector<std::pair<std::string, std::string>> keyValues;
        for (int i = 0, count = req.kvs_size(); i < count; ++i) {
            auto kv = req.kvs(i);
            do {
                if (req.case_() != kvrpcpb::EC_Force) {
                    bool bExists = store_->KeyExists(kv.key());
                    if ((existCase == kvrpcpb::EC_Exists && !bExists) ||
                        (existCase == kvrpcpb::EC_NotExists && bExists)) {
                        break;
                    }
                    if (bExists) {
                        ++affected_keys;
                    }
                }
                total_size += kv.key().size() + kv.value().size();
                ++total_count;

                keyValues.push_back(std::pair<std::string, std::string>(kv.key(), kv.value()));
            } while (false);
        }

        ret = store_->BatchSet(keyValues);
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyKVBatchSet failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            CheckSplit(total_size);
        }
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsKvBatchSetResponse resp;
//            resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//            delete err;
//        }

    return ret;
}

Status RangeBase::ApplyKVDelete(const raft_cmdpb::Command &cmd, errorpb::Error *&err) {
    Status ret;
    //errorpb::Error *err = nullptr;
    auto btime = NowMicros();

    do {
        auto &req = cmd.kv_delete_req();

        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyKVBatchSet error: %s", err->message().c_str());
            break;
        }

        ret = store_->Delete(req.key());
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyKVDelete failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsKvDeleteResponse resp;
//            resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//            delete err;
//        }
    return ret;
}

Status RangeBase::ApplyKVBatchDelete(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;

//        uint64_t affected_keys = 0;
//        errorpb::Error *err = nullptr;
    auto btime = NowMicros();

    do {
        auto &req = cmd.kv_batch_del_req();
        std::vector<std::string> delKeys(req.keys_size());

        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyKVBatchDelete error: %s", err->message().c_str());
            break;
        }

        for (int i = 0, count = req.keys_size(); i < count; ++i) {
            auto &key = req.keys(i);
            if (req.case_() == kvrpcpb::EC_Exists ||
                req.case_() == kvrpcpb::EC_AnyCase) {
                if (store_->KeyExists(key)) {
                    ++affected_keys;
                    delKeys.push_back(std::move(key));
                }
            } else {
                delKeys.push_back(std::move(key));
            }
        }

        ret = store_->BatchDelete(delKeys);
        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, NowMicros() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyKVBatchDelete failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }

    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsKvBatchDeleteResponse resp;
//            resp.mutable_resp()->set_code(static_cast<int>(ret.code()));
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//            delete err;
//        }
    return ret;
}

Status RangeBase::ApplyKVRangeDelete(const raft_cmdpb::Command &cmd, uint64_t& affected_keys, errorpb::Error *&err) {
    Status ret;

//        uint64_t affected_keys = 0;
//        errorpb::Error *err = nullptr;
    std::string last_key;
    auto btime = NowMicros();

    do {
        auto &req = cmd.kv_range_del_req();
        auto start = std::max(req.start(), start_key_);
        auto limit = std::min(req.limit(), meta_.GetEndKey());

        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("KVRangeDelet error: %s", err->message().c_str());
            break;
        }

        if (req.case_() == kvrpcpb::EC_Exists ||
            req.case_() == kvrpcpb::EC_AnyCase) {
            std::unique_ptr<storage::IteratorInterface> iterator(
                    store_->NewIterator(start, limit));
            int maxCount = checkMaxCount(req.max_count());
            std::vector<std::string> delKeys(maxCount);

            for (int i = 0; iterator->Valid() && i < maxCount; ++i) {
                delKeys.push_back(std::move(iterator->key()));
                iterator->Next();
            }

            if (delKeys.size() > 0) {
                last_key = delKeys[delKeys.size() - 1];
            }

            ret = store_->BatchDelete(delKeys);
            affected_keys = delKeys.size();
        } else {
            ret = store_->RangeDelete(start, limit);
        }

        context_->Statistics(slave_flag_)->PushTime(monitor::HistogramType::kStore, NowMicros() - btime);
    } while (false);

//        if (cmd.cmd_id().node_id() == node_id_) {
//            kvrpcpb::DsKvRangeDeleteResponse resp;
//            resp.mutable_resp()->set_last_key(last_key);
//            resp.mutable_resp()->set_affected_keys(affected_keys);
//            resp.mutable_resp()->set_code(static_cast<int32_t>(ret.code()));
//            ReplySubmit(cmd, resp, err, btime);
//        } else if (err != nullptr) {
//            delete err;
//        }

    return ret;
}

Status RangeBase::Destroy() {
    valid_ = false;

    auto s = store_->Truncate();
    if (!s.ok()) {
        RANGE_LOG_ERROR("truncate store fail: %s", s.ToString().c_str());
        return s;
    }

    if (slave_flag_ != 1) {
        context_->Statistics()->ReportLeader(id_, false);

        s = context_->MetaStore()->DeleteApplyIndex(id_);
        if (!s.ok()) {
            RANGE_LOG_ERROR("truncate delete apply fail: %s", s.ToString().c_str());
        }
    } else {
        s = context_->MetaStore()->DeletePersistIndex(id_);
        if (!s.ok()) {
            RANGE_LOG_ERROR("truncate delete apply fail: %s", s.ToString().c_str());
        }
    }
    return s;
}


void RangeBase::CheckSplit(uint64_t size) {
    statis_size_ += size;

    // split disabled
    if (!context_->GetSplitPolicy()->IsEnabled()) {
        return;
    }

    auto check_size = context_->GetSplitPolicy()->CheckSize();
    if (!statis_flag_ && statis_size_ > check_size) {
        statis_flag_ = true;
        context_->ScheduleCheckSize(id_);
    }
}


bool RangeBase::VerifyWriteable(errorpb::Error **err) {
    auto percent = context_->GetDBUsagePercent(slave_flag_);
    if (percent < kStopWriteFsUsagePercent) {
        return true;
    }

    RANGE_LOG_ERROR("filesystem usage percent(%" PRIu64 "> %" PRIu64 ") limit reached, reject write request",
                    percent, kStopWriteFsUsagePercent);

    if (err != nullptr) {
        auto no_left_space_err = new errorpb::Error;
        no_left_space_err ->set_message("no left space");
        no_left_space_err ->mutable_no_left_space();
        *err = no_left_space_err;
    }
    return false;
}

bool RangeBase::KeyInRange(const std::string &key) {
    if (key < start_key_) {
        RANGE_LOG_WARN("key: %s less than start_key:%s, out of range",
                       EncodeToHex(key).c_str(), EncodeToHex(start_key_).c_str());
        return false;
    }

    auto end_key = meta_.GetEndKey();
    if (key >= end_key) {
        RANGE_LOG_WARN("key: %s greater than end_key:%s, out of range",
                       EncodeToHex(key).c_str(), EncodeToHex(end_key).c_str());
        return false;
    }

    return true;
}

bool RangeBase::KeyInRange(const std::string &key, errorpb::Error *&err) {
    if (!KeyInRange(key)) {
        err = KeyNotInRange(key);
        return false;
    }

    return true;
}

errorpb::Error *RangeBase::KeyNotInRange(const std::string &key) {
    auto err = new errorpb::Error;
    err->set_message("key not in range");
    err->mutable_key_not_in_range()->set_range_id(id_);
    err->mutable_key_not_in_range()->set_key(key);
    err->mutable_key_not_in_range()->set_start_key(start_key_);
    // end_key change at range split time
    err->mutable_key_not_in_range()->set_end_key(meta_.GetEndKey());
    return err;
}

errorpb::Error *RangeBase::StaleEpochError(const metapb::RangeEpoch &epoch) {
    auto err = new errorpb::Error;
    std::string msg = "stale epoch, req version:";
    msg += std::to_string(epoch.version());
    msg += " cur version:";
    msg += std::to_string(meta_.GetVersion());

    err->set_message(std::move(msg));
    auto stale_epoch = err->mutable_stale_epoch();
    meta_.Get(stale_epoch->mutable_old_range());

    if (split_range_id_ > 0) {
        auto split_range = context_->FindRange(split_range_id_);
        if (split_range) {
            auto split_meta = split_range->options();
            stale_epoch->set_allocated_new_range(new metapb::Range(std::move(split_meta)));
        }
    }

    return err;
}

bool RangeBase::EpochIsEqual(const metapb::RangeEpoch &epoch) {
    return meta_.GetVersion() == epoch.version();
}

bool RangeBase::EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&err) {
    if (!EpochIsEqual(epoch)) {
        err = StaleEpochError(epoch);
        return false;
    }
    return true;
}

}  // namespace range
}  // namespacBe dataserver
}  // namespace sharkstore
