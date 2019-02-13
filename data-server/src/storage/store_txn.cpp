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

static TxnError* newTxnServerErr(int32_t code, const std::string& msg) {
    auto err = new TxnError;
    err->set_err_type(TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
    return err;
}

Status Store::getTxnValue(const std::string &key, txnpb::TxnValue *value) {
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

bool Store::checkLockable(const std::string& txn_id, const std::string& key, txnpb::TxnError** err) {
    return false;
}

void Store::writeTxnValue(const PrepareRequest& req, const txnpb::TxnIntent& intent,
        rocksdb::WriteBatch* batch, txnpb::TxnError** err) {
    if (intent.check_unique() || intent.expected_ver()) {
        if (intent.check_unique()) {
        }
        if (intent.expected_ver() != 0) {
        }
    }

    // TODO: remove copy
    txnpb::TxnValue txn_value;
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
        *err = newTxnServerErr(Status::kCorruption, "serialize txn value failed");
        return;
    }
    auto ret = batch->Put(txn_cf_, intent.key(), db_value);
    if (!ret.ok()) {
        *err = newTxnServerErr(Status::kIOError, ret.ToString());
        return;
    }
}

void Store::TxnPrepare(const txnpb::PrepareRequest& req, txnpb::PrepareResponse* resp) {
    bool primary_locked = false;
    // check lockable
    for (const auto& intent: req.intents()) {
    }
}

uint64_t Store::TxnDecide(const txnpb::DecideRequest& req, txnpb::DecideResponse* resp) {
    return 0;
}

void Store::TxnClearup(const txnpb::ClearupRequest& req, txnpb::ClearupResponse* resp) {
}

void Store::TxnGetLockInfo(const txnpb::GetLockInfoRequest& req, txnpb::GetLockInfoResponse* resp) {
}

void Store::TxnSelect(const txnpb::SelectRequest& req, txnpb::SelectResponse* resp) {
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
