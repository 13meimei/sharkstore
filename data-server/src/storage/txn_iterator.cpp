#include "txn_iterator.h"

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

TxnIterator::TxnIterator(std::unique_ptr<IteratorInterface> data_iter,
    std::unique_ptr<IteratorInterface> txn_iter) :
    data_iter_(std::move(data_iter)),
    txn_iter_(std::move(txn_iter)) {
}

bool TxnIterator::valid() {
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

Status TxnIterator::Next(std::string& key, std::string& data_value, std::string& intent_value, bool &over) {
    if (!valid()) {
        over = true;
        return status_;
    }

    over = false;

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
        key = std::move(data_key);
        data_value = data_iter_->value();
        data_iter_->Next();
    }

    // read txn iter
    if (has_intent) {
        key = std::move(intent_key);
        intent_value = txn_iter_->value();
        txn_iter_->Next();
    }

    return Status::OK();
}


} // namespace storage
} // namespace dataserver
} // namespace sharkstore
