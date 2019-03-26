#include "iterator.h"

#include "storage/db/mvcc.h"
#include "mass_tree_mvcc.h"
#include "scaner.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MassTreeIterator::MassTreeIterator(std::unique_ptr<Scaner> scaner, uint64_t version, const Releaser& release_func) :
    scaner_(std::move(scaner)),
    ver_(version),
    releaser_(release_func) {
    this->seek();
}

MassTreeIterator::~MassTreeIterator() {
    if (releaser_) {
        releaser_();
    }
}

bool MassTreeIterator::Valid() {
    return scaner_->Valid();
}

void MassTreeIterator::seek() {
    //a3,a2,a1,b4,b2,c5,c4,c3;if v=3;find a3,b2,c3
    MultiVersionKey iter_key;
    while (scaner_->Valid()) {
        iter_key.from_string(scaner_->Key());
        bool different_key = !cur_assigned_ || iter_key.key() != cur_key_.key();
        if (different_key && iter_key.ver() <= ver_) {
            cur_key_.set_key(iter_key.key()); // 选择了该key，但还需要判断是否是delete，不是的话就算拿到
            cur_assigned_ = true;
            if (!iter_key.is_del()) {
                break;
            }
        }
        scaner_->Next();
    }
}

void MassTreeIterator::Next() {
    scaner_->Next();
    this->seek();
}

Status MassTreeIterator::status() {
    return Status::OK();
}

std::string MassTreeIterator::key() {
    return cur_key_.key();
}

std::string MassTreeIterator::value() {
    return scaner_->Value();
}

uint64_t MassTreeIterator::key_size() {
    return cur_key_.key().length();
}

uint64_t MassTreeIterator::value_size() {
    return scaner_->Value().length();
}

}
}
}
