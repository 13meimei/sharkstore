#include "kv_fetcher.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const kvrpcpb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.key(), false));
    } else {
        return std::unique_ptr<KvFetcher>(new RangeKvFetcher(store, req.scope().start(), req.scope().limit()));
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const kvrpcpb::DeleteRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.key(), false));
    } else {
        return std::unique_ptr<KvFetcher>(new RangeKvFetcher(store, req.scope().start(), req.scope().limit()));
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const txnpb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.key(), true));
    } else {
        return std::unique_ptr<KvFetcher>(new TxnRangeKvFetcher(store, req.scope().start(), req.scope().limit()));
    }
}


PointerKvFetcher::PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent) :
    store_(s), key_(key), fetch_intent_(fetch_intent) {
}

Status PointerKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (fetched_) { // only once
        return Status::OK();
    }

    fetched_ = true;

    auto s = store_.Get(key_, &rec.value);
    if (s.ok()) {
        rec.MarkHasValue();
    } else if (s.code() != Status::kNotFound) { // error
        return s;
    }

    if (fetch_intent_) {
        s = store_.GetTxnValue(key_, rec.intent);
        if (s.ok()) {
            rec.MarkHasIntent();
        } else if (s.code() != Status::kNotFound) { // error
            return s;
        }
    }

    if (rec.Valid()) {
        rec.key = key_;
    }

    return Status::OK();
}

RangeKvFetcher::RangeKvFetcher(Store& s, const std::string& start, const std::string& limit) :
    iter_(s.NewIterator(start, limit)) {
    if (iter_ == nullptr) {
        status_ = Status(Status::kIOError, "create iterator failed", "null");
    }
}

Status RangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!status_.ok()) {
        return status_;
    }
    // iter is over
    if (!iter_->Valid()) {
        status_ = iter_->status();
        return status_;
    }

    rec.key = iter_->key();
    rec.value = iter_->value();
    rec.MarkHasValue();
    iter_->Next();

    return status_;
}

TxnRangeKvFetcher::TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit) {
    status_ = s.NewIterators(data_iter_, txn_iter_, start, limit);
}

bool TxnRangeKvFetcher::valid() {
    if (over_ || !status_.ok()) {
        return false;
    }

    auto s = data_iter_->status();
    if (!s.ok()) {
        status_ = std::move(s);
        return false;
    }
    s = txn_iter_->status();
    if (!s.ok()) {
        status_  = std::move(s);
        return false;
    }
    if (!data_iter_->Valid() && !txn_iter_->Valid()) {
        over_ = true;
        return false;
    }
    return true;
}

Status TxnRangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!valid()) {
        return status_;
    }

    // 比较两个迭代器key的大小，取小的那个
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
    if (has_data) {
        rec.key = std::move(data_key);
        rec.value = data_iter_->value();
        rec.MarkHasValue();
        data_iter_->Next();
    }

    // read txn iter
    if (has_intent) {
        rec.key = std::move(intent_key);
        rec.intent = txn_iter_->value();
        rec.MarkHasIntent();
        txn_iter_->Next();
    }

    return Status::OK();
}

} // namespace storage
} // namespace dataserver
} // namespace sharkstore
