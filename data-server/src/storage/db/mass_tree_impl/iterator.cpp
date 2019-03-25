#include "iterator.h"

#include "storage/db/mvcc.h"
#include "mass_tree_mvcc.h"
#include "scaner.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MassTreeIterator::MassTreeIterator(MvccMassTree *db, std::unique_ptr<Scaner> scaner) :
    db_(db),
    scaner_(std::move(scaner)) {
    ver_ = db_->mvcc_.insert();
    if (scaner_->Valid()) {
        cur_key_.from_string(scaner_->Key());
    }
}

MassTreeIterator::~MassTreeIterator() {
    db_->mvcc_.erase(ver_);
}

bool MassTreeIterator::Valid() {
    return scaner_->Valid();
}

void MassTreeIterator::Next() {
    //a3,a2,a1,b4,b2,c5,c4,c3;if v=3;find a3,b2,c3
    MultiVersionKey iter;
    scaner_->Next();
    while (scaner_->Valid()) {
        iter.from_string(scaner_->Key());
        if (cur_key_.key() != iter.key()
            && iter.ver() <= cur_key_.ver()
            && !iter.is_del())
        {
            cur_key_.set_key(iter.key());
            break;
        }
        scaner_->Next();
    }
}

Status MassTreeIterator::status() {
    return Status::OK();
}

std::string MassTreeIterator::key() {
    return scaner_->Key();
}

std::string MassTreeIterator::value() {
    return scaner_->Value();
}

uint64_t MassTreeIterator::key_size() {
    return scaner_->Key().length();
}

uint64_t MassTreeIterator::value_size() {
    return scaner_->Value().length();
}

}
}
}
