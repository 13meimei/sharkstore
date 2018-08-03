#include "range.h"
#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

namespace lock {
bool DecodeKey(std::string& key,
               const std::string& buf) {
    assert(keys.size() == 0 && buf.length() > 9);

    size_t offset;
    for (offset = 9; offset < buf.length();) {
        if (!DecodeBytesAscending(buf, offset, &key)) {
            return false;
        }
    }
    return true;
}

bool DecodeValue(std::string* value,
                 std::string* lock_id,
                 int64_t* expired_time,
                 int64_t* update_time,
                 int64_t* delete_flag,
                 std::string* creator,
                 std::string& buf) {
    assert(version != nullptr && value != nullptr && extend != nullptr &&
           buf.length() != 0);

    size_t offset = 0;
    if (!DecodeBytesValue(buf, offset, value)) return false; // varchar    v
    if (!DecodeBytesValue(buf, offset, lock_id)) return false; // varchar    lock_id
    if (!DecodeIntValue(buf, offset, expired_time)) return false; // bigint     expired_time
    if (!DecodeIntValue(buf, offset, update_time)) return false; // bigint     upd_time
    if (!DecodeIntValue(buf, offset, delete_flag)) return false; // int        delete_flag
    if (!DecodeBytesValue(buf, offset, creator)) return false; // varchar    creator
    return true;
}

void EncodeKey(std::string* buf,
               uint64_t tableId, const std::string* key) {
    assert(buf != nullptr && buf->length() != 0);
    assert(keys.size() != 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    EncodeBytesAscending(buf, key->c_str(), key->length());
}

void EncodeValue(std::string* buf,
                 const std::string* value,
                 const std::string* lock_id,
                 int64_t expired_time,
                 int64_t update_time,
                 int64_t delete_flag,
                 const std::string* creator) {
    assert(buf != nullptr);

    EncodeBytesValue(buf, 2, value->c_str(), value->length());              // varchar    v
    EncodeBytesValue(buf, 3, lock_id->c_str(), lock_id->length());  // varchar    lock_id
    EncodeIntValue(buf, 4, expired_time);                           // bigint     expired_time
    EncodeIntValue(buf, 5, update_time);                            // bigint     upd_time
    EncodeIntValue(buf, 6, delete_flag);                            // int        delete_flag
    EncodeBytesValue(buf, 7, creator->c_str(), creator->length());  // varchar    creator
}

} // namespace lock

kvrpcpb::LockValue *Range::LockGet(const std::string &key) {
    FLOG_DEBUG("lock get: key[%s]", EncodeToHexString(key).c_str());
    std::string val;
    if (!store_->Get(key, &val).ok()) {
        FLOG_WARN("lock get: no key[%s]", EncodeToHexString(key).c_str());
        return nullptr;
    }

    FLOG_DEBUG("lock get ok: key[%s] val[%s]", EncodeToHexString(key).c_str(),
               EncodeToHexString(val).c_str());

    std::string value;
    std::string lock_id;
    int64_t expired_time;
    int64_t update_time;
    int64_t delete_flag;
    std::string creator;
    if (!lock::DecodeValue(&value, &lock_id, &expired_time, &update_time, &delete_flag, &creator,
                                 val)) {
        FLOG_WARN("lock get: decode value failed, key[%s]", EncodeToHexString(key).c_str());
        return nullptr;
    }

    auto ret = new kvrpcpb::LockValue;
    ret->set_value(value);
    ret->set_id(lock_id);
    ret->set_delete_time(expired_time);
    ret->set_update_time(update_time);
    ret->set_delete_flag(delete_flag);
    ret->set_by(creator);

    if (ret->delete_time() > 0 && ret->delete_time() <= getticks()) {
        FLOG_WARN("key[%s] deteled at time %ld", EncodeToHexString(key).c_str(),
                  ret->delete_time());
        return nullptr;
    }

    /*if (getticks() - ret->update_time() > DEFAULT_LOCK_DELETE_TIME_MILLSEC) {
        FLOG_WARN("key[%s] deteled last update time %ld > 3s",
                  EncodeToHexString(key).c_str(), ret->update_time());
        return nullptr;
    }*/

    FLOG_DEBUG("lock get parse: key[%s] val[%s]",
               EncodeToHexString(key).c_str(), ret->DebugString().c_str());
    return ret;
}

void Range::Lock(common::ProtoMessage *msg, kvrpcpb::DsLockRequest &req) {
    FLOG_DEBUG("lock request: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, req.req().table_id(), &req.req().key());
    req.mutable_req()->set_key(encode_key);

    auto& key = req.req().key();

    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", id_, err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", id_, err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", id_, err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Lock);
            cmd.set_allocated_lock_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] Lock raft submit error: %s", id_, ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLock(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply lock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto req = cmd.lock_req();
    auto resp = new (kvrpcpb::DsLockResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyLock error: %s", id_, err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }
        auto val = LockGet(req.key());
        // 允许相同id的重复执行lock
        if (val != nullptr) {
            if (val->delete_flag()) {
                FLOG_WARN("Range %" PRIu64
                          "  ApplyLock error: lock [%s] is force unlocked",
                          id_, req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
                resp->mutable_resp()->set_error("be force unlocked");
                resp->mutable_resp()->set_value(val->value());
                resp->mutable_resp()->set_update_time(val->update_time());
                break;
            }
            if (req.value().id() != val->id()) {
                FLOG_WARN("Range %" PRIu64
                          "  ApplyLock error: lock [%s] is existed",
                          id_, req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_EXISTED);
                resp->mutable_resp()->set_error("already locked");
                resp->mutable_resp()->set_value(val->value());
                resp->mutable_resp()->set_update_time(val->update_time());
                break;
            }
        }

        auto btime = get_micro_second();
        req.mutable_value()->set_update_time(getticks());
        if (req.value().delete_time() != 0) {
            req.mutable_value()->set_delete_time(req.value().delete_time() +
                                                 getticks());
        }

        std::string value_buf;
        const std::string& value = req.value().value();
        const std::string& lock_id = req.value().id();
        int64_t expired_time = req.value().delete_time();
        int64_t update_time = req.value().update_time();
        int64_t delete_flag = req.value().delete_flag();
        const std::string& creator = req.value().by();

        lock::EncodeValue(&value_buf,
                          &value, &lock_id, expired_time, update_time, delete_flag, &creator);
        ret = store_->Put(req.key(), value_buf);

        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyLock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().ByteSizeLong();
            CheckSplit(len);
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyLock: lock [%s] is locked by %s",
                  meta_.id(), req.key().c_str(), req.value().by().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockUpdate(common::ProtoMessage *msg,
                       kvrpcpb::DsLockUpdateRequest &req) {
    FLOG_DEBUG("lock update: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, req.req().table_id(), &req.req().key());
    req.mutable_req()->set_key(encode_key);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", id_, err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", id_, err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", id_, err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::LockUpdate);
            cmd.set_allocated_lock_update_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] LockUpdate raft submit error: %s", id_, ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockUpdateResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLockUpdate(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply lock update: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.lock_update_req();
    auto resp = new (kvrpcpb::DsLockUpdateResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyLockUpdate error: %s",
                      id_, err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] is not existed",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        if (val->delete_flag()) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] is force unlocked",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
            resp->mutable_resp()->set_error("be force unlocked");
            break;
        }
        if (req.id() != val->id()) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] can not update with id "
                      "%s != %s",
                      req.key().c_str(), req.id().c_str(), val->id().c_str());
            resp->mutable_resp()->set_code(LOCK_ID_MISMATCHED);
            resp->mutable_resp()->set_error("wrong id: " + val->id());
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        val->set_update_time(getticks() + req.update_time());
        if (req.update_value().size() != 0) {
            val->set_value(req.update_value());
        }

        auto btime = get_micro_second();

        std::string value_buf;
        const std::string& value = val->value();
        const std::string& lock_id = val->id();
        int64_t expired_time = val->delete_time();
        int64_t update_time = val->update_time();
        int64_t delete_flag = val->delete_flag();
        const std::string& creator = req.by();

        lock::EncodeValue(&value_buf,
                          &value, &lock_id, expired_time, update_time, delete_flag, &creator);
        ret = store_->Put(req.key(), value_buf);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyLockUpdate failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock update failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.ByteSizeLong();
            CheckSplit(len);
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyLockUpdate: lock [%s] is update",
                  id_, req.key().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);

    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::Unlock(common::ProtoMessage *msg, kvrpcpb::DsUnlockRequest &req) {
    FLOG_DEBUG("unlock: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, req.req().table_id(), &req.req().key());
    req.mutable_req()->set_key(encode_key);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Unlock);
            cmd.set_allocated_unlock_req(req.release_req());
        });
        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] Unlock raft submit error: %s", id_, ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] Unlock error: %s", id_, err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockResponse;
        SendError(msg, req.header(), resp, err);
    }
    return;
}

Status Range::ApplyUnlock(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply unlock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.unlock_req();
    auto resp = new (kvrpcpb::DsUnlockResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyUnlock error: %s", id_, err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlock error: lock [%s] is not existed",
                      id_, req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        // 允许force unlock的锁被owner正常解锁
        if (val->delete_flag()) {
            if (req.id() != val->id()) {
                FLOG_WARN("ApplyUnlock error: lock [%s] is force unlocked",
                          req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
                resp->mutable_resp()->set_error("be force unlocked");
                break;
            }
        }
        if (req.id() != val->id()) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlock error: lock [%s] not locked with id %s",
                      id_, req.key().c_str(), req.id().c_str());
            resp->mutable_resp()->set_code(LOCK_ID_MISMATCHED);
            resp->mutable_resp()->set_error("wrong id: " + val->id());
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        auto btime = get_micro_second();
        ret = store_->Delete(req.key());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyUnlock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("unlock failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyUnlock: lock [%s] is unlock by %s",
                  id_, req.key().c_str(), req.by().c_str());

        std::string decode_key;
        auto decode_ret = lock::DecodeKey(decode_key, req.key());
        if (!decode_ret) {
            FLOG_ERROR("ApplyUnlock decode lock key [%s] failed", EncodeToHexString(req.key()).c_str());
        }

        std::string err_msg;
        watchpb::WatchKeyValue watch_kv;
        watch_kv.set_key(0, decode_key);
        auto retCnt = WatchNotify(watchpb::DELETE, watch_kv, err_msg);
        if (retCnt < 0) {
            FLOG_ERROR("ApplyUnlock WatchNotify failed, ret:%d, msg:%s", retCnt, err_msg.c_str());
        } else {
            FLOG_DEBUG("ApplyUnlock WatchNotify success, count:%d, msg:%s", retCnt, err_msg.c_str());
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::UnlockForce(common::ProtoMessage *msg,
                        kvrpcpb::DsUnlockForceRequest &req) {
    FLOG_DEBUG("unlock force: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, req.req().table_id(), &req.req().key());
    req.mutable_req()->set_key(encode_key);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::UnlockForce);
            cmd.set_allocated_unlock_force_req(req.release_req());
        });
        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] UnlockForce raft submit error: %s",
                       id_, ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] UnlockForce error: %s", id_, err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockForceResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyUnlockForce(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply unlock force: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.unlock_force_req();
    auto resp = new (kvrpcpb::DsUnlockForceResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  UnlockForce error: %s", id_, err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlockForce error: lock [%s] is not existed",
                      id_, req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        if (val->delete_flag()) {
            FLOG_WARN("ApplyUnlock error: lock [%s] is force unlocked",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
            resp->mutable_resp()->set_error("be force unlocked");
            break;
        }

        auto btime = get_micro_second();
        // do not really delete until the deleted time
        val->set_delete_flag(1);


        std::string value_buf;
        const std::string& value = val->value();
        const std::string& lock_id = val->id();
        int64_t expired_time = val->delete_time();
        int64_t update_time = val->update_time();
        int64_t delete_flag = val->delete_flag();
        const std::string& creator = req.by();

        lock::EncodeValue(&value_buf,
                          &value, &lock_id, expired_time, update_time, delete_flag, &creator);
        ret = store_->Put(req.key(), value_buf);

        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyUnlockForce failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("unlock force failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        FLOG_INFO("Range %" PRIu64
                  "  ApplyUnlockForce: lock [%s] is force unlocked by %s",
                  id_, req.key().c_str(), req.by().c_str());

        std::string decode_key;
        auto decode_ret = lock::DecodeKey(decode_key, req.key());
        if (!decode_ret) {
            FLOG_ERROR("ApplyUnlockForce decode lock key [%s] failed", EncodeToHexString(req.key()).c_str());
        }

        std::string err_msg;
        watchpb::WatchKeyValue watch_kv;
        watch_kv.set_key(0, decode_key);
        auto retCnt = WatchNotify(watchpb::DELETE, watch_kv, err_msg);
        if (retCnt < 0) {
            FLOG_ERROR("ApplyUnlockForce WatchNotify failed, ret:%d, msg:%s", retCnt, err_msg.c_str());
        } else {
            FLOG_DEBUG("ApplyUnlockForce WatchNotify success, count:%d, msg:%s", retCnt, err_msg.c_str());
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockScan(common::ProtoMessage *msg, kvrpcpb::DsLockScanRequest &req) {
    FLOG_DEBUG("lock scan: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait, get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsLockScanResponse;
    auto start = std::max(req.req().start(), meta_.start_key());
    auto limit = std::min(req.req().limit(), meta_.end_key());
    std::unique_ptr<storage::Iterator> iterator(store_->NewIterator(start, limit));

    int max_count = checkMaxCount(static_cast<int64_t >(req.req().count()));
    auto resp = ds_resp->mutable_resp();

    uint64_t count = 0;
    uint64_t total_size = 0;

    for (int i = 0; iterator->Valid() && i < max_count; ++i) {
        auto kv = resp->add_info();
        FLOG_DEBUG("scan key: %s", iterator->key().c_str());
        kv->set_key(std::move(iterator->key()));
        kv->mutable_value()->ParseFromString(iterator->value());

        count++;
        total_size += iterator->key().length()+iterator->value().length();

        iterator->Next();
    }

    if (resp->info_size() > 0) {
        auto lastIdx = resp->info_size()-1;
        auto lastInfo = resp->info(lastIdx);
        resp->set_last_key(lastInfo.key());
        FLOG_DEBUG("last key: %s", lastInfo.key().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), ds_resp->mutable_header(), err);
    context_->socket_session->Send(msg, ds_resp);
}

}  // namespace range
}
}
