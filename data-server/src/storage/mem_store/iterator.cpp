#include "iterator.h"
#include <assert.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

MemIterator::MemIterator(memstore::Iterator<std::string, std::string>* it,
                         const std::string& start, const std::string& limit)
    :rit_(it), limit_(limit) {
    assert(!start.empty());
    assert(!limit.empty());
}

MemIterator::~MemIterator() {}

bool MemIterator::Valid() { return rit_->Valid(); }

void MemIterator::Next() { rit_->Next(); }

Status MemIterator::status() {
    // todo
    return Status::OK();
}

std::string MemIterator::key() { return rit_->Key(); }

std::string MemIterator::value() { return rit_->Value(); }

uint64_t MemIterator::key_size() { return rit_->Key().size(); }

uint64_t MemIterator::value_size() { return rit_->Value().size(); }

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
