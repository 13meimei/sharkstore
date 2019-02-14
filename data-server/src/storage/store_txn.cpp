#include "store.h"

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

using namespace txnpb;
using namespace std::chrono;

static uint64_t calExpireAt(uint64_t ttl) {
    auto seconds = system_clock::now().time_since_epoch();
    return ttl + duration_cast<milliseconds>(seconds).count();
}

static bool isExpired(uint64_t expired_at) {
    auto seconds = system_clock::now().time_since_epoch();
    auto now = duration_cast<milliseconds>(seconds).count();
    return static_cast<uint64_t>(now) > expired_at;
}

static void setTxnServerErr(TxnError* err, int32_t code, const std::string& msg) {
    err->set_err_type(TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
}

static TxnErrorPtr newTxnServerErr(int32_t code, const std::string& msg) {
    TxnErrorPtr err(new TxnError);
    setTxnServerErr(err.get(), code, msg);
    return err;
}

static TxnErrorPtr newLockedError(const TxnValue& value) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_LOCKED);

    auto lock_err = err->mutable_lock_err();
    lock_err->set_key(value.intent().key());

    auto lock_info = lock_err->mutable_info();
    lock_info->set_txn_id(value.txn_id());
    lock_info->set_timeout(isExpired(value.expired_at()));
    lock_info->set_is_primary(value.intent().is_primary());
    lock_info->set_primary_key(value.primary_key());
    if (value.intent().is_primary()) {
        lock_info->set_status(value.txn_status());
        for (const auto& skey: value.secondary_keys()) {
            lock_info->add_secondary_keys(skey);
        }
    }
    return err;
}

// TODO: load from memory
Status Store::getTxnValue(const std::string &key, TxnValue *value) {
    std::string db_value;
    auto s = db_->Get(rocksdb::ReadOptions(), txn_cf_, key, &db_value);
    if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else if (!s.ok()) {
        return Status(Status::kIOError, "Get Txn", s.ToString());
    }
    if (!value->ParseFromString(db_value)) {
        return Status(Status::kCorruption, "parse txn value", EncodeToHex(db_value));
    }
    return Status::OK();
}

TxnErrorPtr Store::checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag) {
    TxnValue value;
    auto s = getTxnValue(key, &value);
    switch (s.code()) {
    case Status::kNotFound:
        return nullptr;
    case Status::kOk:
        assert(value.intent().key() == key);
        if (value.txn_id() == txn_id) {
            *exist_flag = true;
            return nullptr;
        } else {
            return newLockedError(value);
        }
    default:
        return newTxnServerErr(s.code(), s.ToString());
    }
}

TxnErrorPtr Store::checkUniqueAndVersion(const txnpb::TxnIntent& intent) {
    // TODO:
    return nullptr;
}

TxnErrorPtr Store::prepareIntent(const PrepareRequest& req, const TxnIntent& intent, rocksdb::WriteBatch* batch) {
    // check lockable
    bool exist_flag = false;
    auto err = checkLockable(intent.key(), req.txn_id(), &exist_flag);
    if (err != nullptr) {
        return err;
    }
    if (exist_flag) { // lockable, intent is already written
        return nullptr;
    }

    // check unique and version
    if (intent.check_unique() || intent.expected_ver()) {
        err = checkUniqueAndVersion(intent);
        if (err != nullptr) {
            return err;
        }
    }

    // append to batch
    // TODO: remove copy
    TxnValue txn_value;
    txn_value.set_txn_id(req.txn_id());
    txn_value.mutable_intent()->CopyFrom(intent);
    txn_value.set_primary_key(req.primary_key());
    txn_value.set_expired_at(calExpireAt(req.lock_ttl()));
    if (intent.is_primary()) {
        for (const auto& key: req.secondary_keys()) {
            txn_value.add_secondary_keys(key);
        }
    }
    std::string db_value;
    if (!txn_value.SerializeToString(&db_value)) {
        return newTxnServerErr(Status::kCorruption, "serialize txn value failed");
    }
    auto ret = batch->Put(txn_cf_, intent.key(), db_value);
    if (!ret.ok()) {
        return newTxnServerErr(Status::kCorruption, "serialize txn value failed");
    }
    return nullptr;
}

void Store::TxnPrepare(const PrepareRequest& req, PrepareResponse* resp) {
    bool primary_lockable = true;
    rocksdb::WriteBatch batch;
    for (const auto& intent: req.intents()) {
        bool stop_flag = false;
        auto err = prepareIntent(req, intent, &batch);
        if (err != nullptr) {
            if (err->err_type() == TxnError_ErrType_LOCKED) {
                if (intent.is_primary()) {
                    primary_lockable = false;
                }
            } else { // 其他类型错误，终止prepare
                resp->clear_errors(); // 清除其他错误
                stop_flag = true;
            }
            resp->add_errors()->Swap(err.get());
        }
        if (stop_flag) break;
    }

    if (primary_lockable) {
        auto ret = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!ret.ok()) {
            resp->clear_errors();
            setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        }
    }
}

uint64_t Store::TxnDecide(const DecideRequest& req, DecideResponse* resp) {
    return 0;
}

void Store::TxnClearup(const ClearupRequest& req, ClearupResponse* resp) {
}

void Store::TxnGetLockInfo(const GetLockInfoRequest& req, GetLockInfoResponse* resp) {
}

void Store::TxnSelect(const SelectRequest& req, SelectResponse* resp) {
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
