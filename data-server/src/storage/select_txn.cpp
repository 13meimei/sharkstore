#include "select_txn.h"

#include "base/util.h"
#include "common/ds_encoding.h"
#include "frame/sf_logger.h"
#include "util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

static const size_t kIteratorTooManyKeys = 1000;

TxnRowFetcher::TxnRowFetcher(Store& s, const txnpb::SelectRequest& req):
    store_(s),
    req_(req),
    decoder_(s.GetPrimaryKeys(), req) {
}

Status TxnRowFetcher::getRow(const std::string& key, const std::string& data_value,
        const std::string& intent_value, txnpb::Row& row) {
    if (!intent_value.empty()) {
        txnpb::TxnValue txn_value;
        if (!txn_value.ParseFromString(intent_value)) {
            return Status(Status::kCorruption, "parse txn value", EncodeToHex(intent_value));
        }

        const auto& intent = txn_value.intent();
        assert(key == intent.key());
        FLOG_DEBUG("select txn intent: %s", intent.ShortDebugString().c_str());
        if (intent.is_primary()) { // primary可以直接确定当前事务的状态
            auto txn_status = txn_value.txn_status();
            switch (txn_status) {
            case txnpb::COMMITTED:
                if (intent.typ() == txnpb::INSERT) { // 使用intent里的value
                    return getRow(key, intent.value(), "", row);
                } else { // 被删除
                    return Status(Status::kNotFound);
                }
            case txnpb::ABORTED:
                return getRow(key, data_value, "", row);
            case txnpb::INIT:
                return getRow(key, data_value, "", row);
            default:
                return Status(Status::kInvalidArgument, "txn status", std::to_string(txn_status));
            }
        } else {
            auto s = addIntent(txn_value, row);
            if (!s.ok()) {
                return s;
            }
        }
    }

    if (!data_value.empty()) {
        auto s = addDefault(key, data_value, row);
        if (!s.ok()) {
            return s;
        }
    }

    if (row.has_value() || row.has_intent()) {
        row.set_key(key);
    } else {
        return Status(Status::kNotFound);
    }
    return Status::OK();
}

Status TxnRowFetcher::addDefault(const std::string& key, const std::string& buf, txnpb::Row& row) {
    TxnRowValue value;
    bool matched = false;
    auto s = decoder_.DecodeAndFilter(key, buf, value, matched);
    if (!s.ok()) {
        return s;
    }
    if (!matched) {
        return Status::OK();
    }
    value.Encode(req_, row.mutable_value());
    return Status::OK();
}

Status TxnRowFetcher::addIntent(const txnpb::TxnValue &txn_value, txnpb::Row &row) {
    if (txn_value.intent().typ() == txnpb::INSERT) {
        TxnRowValue value;
        bool matched = false;
        auto s = decoder_.DecodeAndFilter(txn_value.intent().key(), txn_value.intent().value(), value, matched);
        if (!s.ok()) {
            return s;
        }
        if (!matched) {
            return Status::OK();
        }
        // TODO: 如果不match，则应该把intent视作为delete
        value.Encode(req_, row.mutable_intent()->mutable_value());
    }

    auto row_intent = row.mutable_intent();
    row_intent->set_txn_id(txn_value.txn_id());
    row_intent->set_op_type(txn_value.intent().typ());
    const auto& primary = txn_value.intent().is_primary() ? txn_value.intent().key(): txn_value.primary_key();
    row_intent->set_primary_key(primary);
    row_intent->set_timeout(isExpired(txn_value.expired_at()));
    return Status::OK();
}

std::unique_ptr<TxnRowFetcher> NewTxnRowFetcher(Store& s, const txnpb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<TxnRowFetcher>(new PointRowFetcher(s, req));
    } else {
        return std::unique_ptr<TxnRowFetcher>(new RangeRowFetcher(s, req));
    }
}

/// PointRowFetcher
PointRowFetcher::PointRowFetcher(Store& s, const txnpb::SelectRequest& req) :
    TxnRowFetcher(s, req) {
    assert(!req.key().empty());
}

Status PointRowFetcher::Next(txnpb::Row& row, bool& over) {
    if (fetched_) { // only once
        over = true;
        return Status::OK();
    }

    fetched_ = true;

    std::string data_value; // value里有version，只有Get到，data_value肯定不会为空字符串
    auto s = store_.Get(req_.key(), &data_value);
    if (s.code() == Status::kNotFound) {
        data_value.clear();
    } else if (!s.ok()) {
        return s; // error
    }

    std::string intent_value;
    s = store_.GetTxnValue(req_.key(), intent_value);
    if (s.code() == Status::kNotFound) {
        intent_value.clear();
    } else if (!s.ok()) {
        return s; // error
    }

    s = getRow(req_.key(), data_value, intent_value, row);
    if (s.code() == Status::kNotFound) {
        over = true;
        return Status::OK();
    } else {
        return s;
    }
}

/// RangeRowFetcher
RangeRowFetcher::RangeRowFetcher(Store& s, const txnpb::SelectRequest& req) :
    TxnRowFetcher(s, req) {
    last_status_ = store_.NewIterators(data_iter_, txn_iter_,
            req_.scope().start(), req_.scope().limit());
}

Status RangeRowFetcher::Next(txnpb::Row& row, bool& over) {
    while (last_status_.ok() && !over_) {
        if (tryGetRow(row)) {
            break;
        }
    }

    ++iter_count_;
    if (iter_count_ % kIteratorTooManyKeys == kIteratorTooManyKeys - 1) {
        FLOG_WARN("iterator too many keys(%" PRIu64 ", req: %s",
                iter_count_, req_.ShortDebugString().c_str());
    }

    over = over_;
    return last_status_;
}

bool RangeRowFetcher::checkIterValid() {
    auto s = data_iter_->status();
    if (!s.ok()) {
        last_status_ = std::move(s);
        return false;
    }
    if (!s.ok()) {
        last_status_ = std::move(s);
        return false;
    }
    if (!data_iter_->Valid() && !txn_iter_->Valid()) {
        over_ = true;
        return false;
    }
    return true;
}

bool RangeRowFetcher::tryGetRow(txnpb::Row &row) {
    if (!checkIterValid()) {
        return false;
    }

    assert(data_iter_->Valid() || txn_iter_->Valid());

    bool has_data = false, has_intent = false;
    std::string data_key, intent_key;
    auto both_valid = data_iter_->Valid() && txn_iter_->Valid();
    if (both_valid) {
        data_key = data_iter_->key();
        intent_key = txn_iter_->key();
        if (data_key == intent_key) { // 都取
            has_data = true;
            has_intent = true;
        } else if (data_key < intent_key) { // 取小的
            has_data = true;
        } else {
            has_intent = true;
        }
    } else if (data_iter_->Valid()) { // only data valid
        data_key = data_iter_->key();
        has_data = true;
    } else { // only intent valid
        intent_key = txn_iter_->key();
        has_intent = true;
    }

    // read data iter
    std::string key;
    std::string data_value;
    if (has_data) {
        key = std::move(data_key);
        data_value = data_iter_->value();
        data_iter_->Next();
    }

    // read txn iter
    std::string intent_value;
    if (has_intent) {
        key = std::move(intent_key);
        intent_value = txn_iter_->value();
        txn_iter_->Next();
    }

    auto s = getRow(key, data_value, intent_value, row);
    if (s.code() == Status::kNotFound) {
        return false;
    } else if (!s.ok()) {
        last_status_ = std::move(s);
        return false;
    } else {
        return true;
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
