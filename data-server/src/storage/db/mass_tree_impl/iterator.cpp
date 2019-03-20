#include "iterator.h"
#include "scaner.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

bool MassTreeIterator::Valid() {
    return scaner_.Valid();
}

void MassTreeIterator::Next() {
    const auto& kv = scaner_.Next();
    it_key_ = kv.first;
    it_value_ = kv.second;
}

std::string MassTreeIterator::key() {
    return it_key_;
}

std::string MassTreeIterator::value() {
    return it_value_;
}

uint64_t MassTreeIterator::key_size() {
    return it_key_.length();
}

uint64_t MassTreeIterator::value_size() {
    return it_value_.length();
};

}
}
}
