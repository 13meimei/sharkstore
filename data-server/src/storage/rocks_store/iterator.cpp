#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

RocksIterator::RocksIterator(rocksdb::RocksIterator* it, const std::string& start,
                   const std::string& limit)
    : rit_(it), limit_(limit) {
    assert(!start.empty());
    assert(!limit.empty());
    rit_->Seek(start);
}

RocksIterator::~RocksIterator() { delete rit_; }

bool RocksIterator::Valid() { return rit_->Valid() && (key() < limit_); }

void RocksIterator::Next() { rit_->Next(); }

Status RocksIterator::status() {
    if (!rit_->status().ok()) {
        return Status(Status::kIOError, rit_->status().ToString(), "");
    }
    return Status::OK();
}

std::string RocksIterator::key() { return rit_->key().ToString(); }

std::string RocksIterator::value() { return rit_->value().ToString(); }

uint64_t RocksIterator::key_size() { return rit_->key().size(); }

uint64_t RocksIterator::value_size() { return rit_->value().size(); }

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
