#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

bool MassTreeIterator::Valid() {
    return scaner_.Valid();
}

void MassTreeIterator::Next() {
    scaner_.Next();
}

Status MassTreeIterator::status() {
    return Status::OK();
}

std::string MassTreeIterator::key() {
    return scaner_.Key();
}

std::string MassTreeIterator::value() {
    return scaner_.Value();
}

uint64_t MassTreeIterator::key_size() {
    return scaner_.Key().length();
}

uint64_t MassTreeIterator::value_size() {
    return scaner_.Value().length();
};

}
}
}
