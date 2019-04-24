#include "store.h"

#include "base/util.h"
#include "frame/sf_logger.h"
#include "common/ds_encoding.h"

#include "select_txn.h"
#include "util.h"
#include "txn_iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

using namespace txnpb;

// TODO: add metrics
static void fillTxnValue(const PrepareRequest& req, const TxnIntent& intent, uint64_t version, TxnValue* value) {
    value->set_txn_id(req.txn_id());
    value->mutable_intent()->CopyFrom(intent);
    value->set_primary_key(req.primary_key());
    value->set_expired_at(calExpireAt(req.lock_ttl()));
    value->set_version(version);
    if (intent.is_primary()) {
        for (const auto& key: req.secondary_keys()) {
            value->add_secondary_keys(key);
        }
    }
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

static void fillLockInfo(LockInfo* lock_info, const TxnValue& value) {
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
}

static TxnErrorPtr newLockedError(const TxnValue& value) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_LOCKED);
    auto lock_err = err->mutable_lock_err();
    lock_err->set_key(value.intent().key());
    fillLockInfo(lock_err->mutable_info(), value);
    return err;
}

static TxnErrorPtr newStatusConflictErr(TxnStatus status) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_STATUS_CONFLICT);
    err->mutable_status_conflict()->set_status(status);
    return err;
}

static void setNotFoundErr(TxnError* err, const std::string& key) {
    err->set_err_type(TxnError_ErrType_NOT_FOUND);
    err->mutable_not_found()->set_key(key);
}

static TxnErrorPtr newNotFoundErr(const std::string& key) {
    TxnErrorPtr err(new TxnError);
    setNotFoundErr(err.get(), key);
    return err;
}

static TxnErrorPtr newUnexpectedVerErr(const std::string& key, uint64_t expected, uint64_t actual) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_UNEXPECTED_VER);
    err->mutable_unexpected_ver()->set_key(key);
    err->mutable_unexpected_ver()->set_expected_ver(expected);
    err->mutable_unexpected_ver()->set_actual_ver(actual);
    return err;
}

static TxnErrorPtr newNotUniqueErr(const std::string& key) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_NOT_UNIQUE);
    err->mutable_not_unique()->set_key(key);
    return err;
}

static TxnErrorPtr newTxnConflictErr(const std::string& expected_txn_id, const std::string& actual_txn_id) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_TXN_CONFLICT);
    err->mutable_txn_conflict()->set_expected_txn_id(expected_txn_id);
    err->mutable_txn_conflict()->set_actual_txn_id(actual_txn_id);
    return err;
}

// TODO: load from memory
Status Store::GetTxnValue(const std::string& key, std::string& db_value) {
    return db_->Get(db_->TxnCFHandle(), key, &db_value);
}

Status Store::GetTxnValue(const std::string &key, TxnValue *value) {
    std::string db_value;
    auto s = GetTxnValue(key, db_value);
    if (!s.ok()) {
        return s;
    }
    if (!value->ParseFromString(db_value)) {
        return Status(Status::kCorruption, "parse txn value", EncodeToHex(db_value));
    }
    assert(value->intent().key() == key);
    return Status::OK();
}

std::unique_ptr<TxnIterator> Store::NewTxnIterator(const std::string& start, const std::string& limit) {
    std::unique_ptr<IteratorInterface> data_iter, txn_iter;
    auto s = NewIterators(data_iter, txn_iter, start, limit);
    if (!s.ok()) {
        return nullptr;
    }
    std::unique_ptr<TxnIterator> iter(new TxnIterator(std::move(data_iter), std::move(txn_iter)));
    return iter;
}

Status Store::writeTxnValue(const txnpb::TxnValue& value, WriteBatchInterface* batch) {
    std::string db_value;
    if (!value.SerializeToString(&db_value)) {
        return Status(Status::kCorruption, "serialize txn value", value.ShortDebugString());
    }
    assert(!value.intent().key().empty());
    auto s = batch->Put(db_->TxnCFHandle(), value.intent().key(), db_value);
    if (!s.ok()) {
        return Status(Status::kIOError, "put txn value", s.ToString());
    } else {
        return Status::OK();
    }
}

TxnErrorPtr Store::checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag) {
    TxnValue value;
    auto s = GetTxnValue(key, &value);
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

static Status decodeTxnVersion(const std::string& value, uint64_t *version) {
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t tag_offset = 0;
    for (size_t offset = 0; offset < value.size();) {
        tag_offset = offset;
        if (!DecodeValueTag(value, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(value));
        }
        if (col_id != kVersionColumnID) {
            if (!SkipValue(value, offset)) {
                return Status(Status::kCorruption,
                              std::string("skip value tag failed at offset ") + std::to_string(offset),
                              EncodeToHexString(value));
            }
        } else {
            int64_t iversion = 0;
            if (!DecodeIntValue(value, offset, &iversion)) {
                return Status(Status::kCorruption,
                              std::string("decode int value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(value));
            } else {
                *version = static_cast<uint64_t>(iversion);
                return Status::OK();
            }
        }
    }
    return Status::OK();
}

Status Store::getKeyVersion(const std::string& key, uint64_t *version) {
    std::string value;
    auto s = this->Get(key, &value);
    if (!s.ok()) {
        return s;
    }
    return decodeTxnVersion(value, version);
}

TxnErrorPtr Store::checkUniqueAndVersion(const txnpb::TxnIntent& intent) {
    uint64_t version = 0;
    auto s = getKeyVersion(intent.key(), &version);
    if (!s.ok() && s.code() != Status::kNotFound) {
        return newTxnServerErr(s.code(), s.ToString());
    }

    if (intent.check_unique() && s.ok()) {
        return newNotUniqueErr(intent.key());
    }

    if (intent.expected_ver() > 0 && version != intent.expected_ver()) {
        return newUnexpectedVerErr(intent.key(), intent.expected_ver(), version);
    }
    return nullptr;
}


uint64_t Store::prepareLocal(const txnpb::PrepareRequest& req, uint64_t version, txnpb::PrepareResponse* resp) {
    assert(req.local());
    auto batch = db_->NewBatch();
    uint64_t bytes_written = 0;
    bool exist_flag = false;
    for (const auto& intent: req.intents()) {
        // check lockable
        auto err = checkLockable(intent.key(), req.txn_id(), &exist_flag);
        if (err != nullptr) {
            resp->add_errors()->Swap(err.get());
            return 0;
        }
        if (exist_flag) {
            setTxnServerErr(resp->add_errors(), Status::kExisted, "lock already exist");
            return 0;
        }

        // check unique and version
        if (intent.check_unique() || intent.expected_ver() != 0) {
            err = checkUniqueAndVersion(intent);
            if (err != nullptr) {
                resp->add_errors()->Swap(err.get());
                return 0;
            }
        }
        // commit directly
        auto s = commitIntent(intent, version, bytes_written, batch.get());
        if (!s.ok()) {
            setTxnServerErr(resp->add_errors(), s.code(), s.ToString());
            return 0;
        }
    }

    auto ret = db_->Write(batch.get());
    if (!ret.ok()) {
        setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        return 0;
    } else {
        addMetricWrite(req.intents_size(), bytes_written);
        return bytes_written;
    }
}

TxnErrorPtr Store::prepareIntent(const PrepareRequest& req, const TxnIntent& intent,
                                 uint64_t version, WriteBatchInterface* batch) {
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
    if (intent.check_unique() || intent.expected_ver() != 0) {
        err = checkUniqueAndVersion(intent);
        if (err != nullptr) {
            return err;
        }
    }

    // append to batch
    TxnValue txn_value;
    fillTxnValue(req, intent, version, &txn_value);
    auto s = writeTxnValue(txn_value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), "serialize txn value failed");
    }
    return nullptr;
}

uint64_t Store::TxnPrepare(const PrepareRequest& req, uint64_t version, PrepareResponse* resp) {
    if (req.local()) {
        return prepareLocal(req, version, resp);
    }

    bool primary_success = true;
    bool fatal_error = false;
    auto batch = db_->NewBatch();
    for (const auto& intent: req.intents()) {
        auto err = prepareIntent(req, intent, version, batch.get());
        if (err != nullptr) {
            if (err->err_type() == TxnError_ErrType_LOCKED) {
                if (intent.is_primary()) {
                    primary_success = false;
                }
            } else { // 其他类型错误，终止prepare，视作整个请求失败
                resp->clear_errors(); // 清除其他错误，只返回关键错误
                fatal_error = true;
            }
            resp->add_errors()->Swap(err.get());
        }
        if (fatal_error) break;
    }

    // 只要primary可以prepare成功就执行写入
    if (!fatal_error && primary_success) {
        auto ret = db_->Write(batch.get());
        if (!ret.ok()) {
            resp->clear_errors();
            setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        }
    }
    return 0;
}

Status Store::commitIntent(const txnpb::TxnIntent& intent, uint64_t version,
                    uint64_t &bytes_written, WriteBatchInterface* batch) {
    Status s ;
    switch (intent.typ()) {
    case DELETE:
        s = batch->Delete(intent.key());
        break;
    case INSERT: {
        // append version field
        std::string db_value = intent.value();
        EncodeIntValue(&db_value, kVersionColumnID, static_cast<int64_t>(version));
        s = batch->Put(intent.key(), db_value);
        bytes_written += intent.key().size() + db_value.size();
        break;
    }
    default:
        return Status(Status::kInvalidArgument, "intent type", std::to_string(intent.typ()));
    }
    if (!s.ok()) {
        return Status(Status::kIOError, "commit intent", s.ToString());
    }
    return Status::OK();
}

TxnErrorPtr Store::decidePrimary(const txnpb::DecideRequest& req, uint64_t& bytes_written,
                          WriteBatchInterface* batch, txnpb::DecideResponse* resp) {
    assert(req.is_primary());

    if (req.keys_size() != 1) {
        return newTxnServerErr(Status::kInvalidArgument,
                std::string("invalid key size: ") + std::to_string(req.keys_size()));
    }

    TxnValue value;
    const auto& key = req.keys(0);
    auto s = GetTxnValue(key, &value);
    if (!s.ok()) {
        if (s.code() == Status::kNotFound) {
            return newNotFoundErr(key);
        } else {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    FLOG_INFO("decide txn primary: get old lock info: %s, req: %s",
                             value.ShortDebugString().c_str(), req.ShortDebugString().c_str());
    // s is ok now
    assert(s.ok());
    if (value.txn_id() != req.txn_id()) {
        return newTxnConflictErr(req.txn_id(), value.txn_id());
    }

    // assign secondary_keys in recover mode
    if (req.recover()) {
        for (const auto& skey: value.secondary_keys()) {
            resp->add_secondary_keys(skey);
        }
    }

    if (value.txn_status() != txnpb::INIT) {
        if (value.txn_status() != req.status()) {
            return newStatusConflictErr(value.txn_status());
        } else { // already decided
            return nullptr;
        }
    }

    // txn status is INIT now
    assert(value.txn_status() == txnpb::INIT);
    // update to new status;
    auto new_value = value;
    new_value.set_txn_status(req.status());
    s = writeTxnValue(new_value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), s.ToString());
    }
    // commit intent
    if (req.status() == COMMITTED) {
        s = commitIntent(value.intent(), value.version(), bytes_written, batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    return nullptr;
}

TxnErrorPtr Store::decideSecondary(const txnpb::DecideRequest& req, const std::string& key, uint64_t& bytes_written,
                            WriteBatchInterface* batch) {
    assert(!req.is_primary());

    TxnValue value;
    auto s = GetTxnValue(key, &value);
    if (!s.ok()) {
        if (s.code() == Status::kNotFound) {
            return nullptr;
        } else {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }
    // s is ok now
    assert(s.ok());
    if (value.txn_id() != req.txn_id()) {
        return nullptr;
    }

    auto ret = batch->Delete(db_->TxnCFHandle(), value.intent().key());
    if (!ret.ok()) {
        return newTxnServerErr(Status::kIOError, ret.ToString());
    }
    if (req.status() == COMMITTED) {
        s = commitIntent(value.intent(), value.version(), bytes_written, batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }
    return nullptr;
}

uint64_t Store::TxnDecide(const DecideRequest& req, DecideResponse* resp) {
    if (req.status() != COMMITTED && req.status() != ABORTED) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "invalid txn status");
        return 0;
    }

    uint64_t bytes_written = 0;
    auto batch = db_->NewBatch();

    TxnErrorPtr err;
    if (req.is_primary()) {
        err = decidePrimary(req, bytes_written, batch.get(), resp);
    } else {
        for (const auto& key: req.keys()) {
            err = decideSecondary(req, key, bytes_written, batch.get());
            if (err != nullptr) {
                break;
            }
        }
    }

    // decide error
    if (err != nullptr) {
        resp->mutable_err()->Swap(err.get());
        return 0;
    }

    auto ret = db_->Write(batch.get());
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return 0;
    } else {
        addMetricWrite(req.keys_size(), bytes_written);
        return bytes_written;
    }
}

void Store::TxnClearup(const ClearupRequest& req, ClearupResponse* resp) {
    txnpb::TxnValue value;
    auto s = GetTxnValue(req.primary_key(), &value);
    if (!s.ok()) {
        if (s.code() != Status::kNotFound) {
            setTxnServerErr(resp->mutable_err(), s.code(), s.ToString());
            return;
        } else {
            return; // not found, success
        }
    }
    // s is ok now
    if (value.txn_id() != req.txn_id()) { // success
        return;
    }
    if (!value.intent().is_primary()) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "target key is not primary");
        return;
    }

    // delete intent
    auto ret = db_->Delete(db_->TxnCFHandle(), req.primary_key());
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return;
    }
}

void Store::TxnGetLockInfo(const GetLockInfoRequest& req, GetLockInfoResponse* resp) {
    TxnValue value;
    auto ret = GetTxnValue(req.key(), &value);
    if (!ret.ok()) {
        if (ret.code() == Status::kNotFound) {
            setNotFoundErr(resp->mutable_err(), req.key());
        } else {
            setTxnServerErr(resp->mutable_err(), ret.code(), ret.ToString());
        }
        return;
    }
    // ret is ok now
    fillLockInfo(resp->mutable_info(), value);
}

Status Store::TxnSelect(const SelectRequest& req, SelectResponse* resp) {
    // currently aggregation is not supported
    for (int i = 0; i < req.field_list_size(); ++i) {
        auto type = req.field_list(i).typ();
        if (type == kvrpcpb::SelectField_Type_AggreFunction) {
            return Status(Status::kNotSupported);
        } else if (type != kvrpcpb::SelectField_Type_Column) {
            return Status(Status::kInvalidArgument, "unknown select field type",
                    kvrpcpb::SelectField_Type_Name(type));
        }
    }

    auto fetcher = NewTxnRowFetcher(*this, req);
    Status s;
    bool over = false;
    uint64_t count = 0;
    uint64_t all = 0;
    uint64_t limit = req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;
    while (!over && s.ok()) {
        over = false;
        std::unique_ptr<txnpb::Row> row(new txnpb::Row);
        s = fetcher->Next(*row, over);
        if (s.ok() && !over) {
            ++all;
            if (all > offset) {
                resp->add_rows()->Swap(row.get());
                if (++count >= limit) break;
            }
        }
    }
    resp->set_offset(all);
    return s;
}

Status Store::TxnScan(const txnpb::ScanRequest& req, txnpb::ScanResponse* resp) {
    auto iter = NewTxnIterator(req.start_key(), req.end_key());
    if (iter == nullptr) {
        return Status(Status::kIOError, "new iterators", "");
    }

    int64_t count = 0;
    bool over = false;
    Status s;
    while (!over) {
        std::string key, data_value, txn_value;
        s = iter->Next(key, data_value, txn_value, over);
        if (!s.ok() || over) {
            break;
        }

        auto kv = resp->add_kvs();
        kv->set_key(std::move(key));

        bool has_del_intent = false;
        bool has_value = false;
        if (!data_value.empty()) {
            kv->set_value(std::move(data_value));
            has_value = true;
        }

        if (!txn_value.empty()) {
            txnpb::TxnValue tv;
            if (!tv.ParseFromString(txn_value)) {
                return Status(Status::kCorruption, "parse txn value", EncodeToHex(txn_value));
            }
            // TODO: use move
            has_del_intent = tv.intent().typ() == txnpb::DELETE;
            auto intent = kv->mutable_intent();
            intent->set_op_type(tv.intent().typ());
            intent->set_txn_id(tv.txn_id());
            intent->set_primary_key(tv.primary_key());
            intent->set_value(tv.intent().value());
        }

        if (has_value && !has_del_intent) { // 有可能这条不算
            ++count;
        }
        if (count >= req.max_count()) {
            break;
        }
    }
    return s;
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
