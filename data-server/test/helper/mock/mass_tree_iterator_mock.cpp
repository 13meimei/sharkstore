#include "mass_tree_iterator_mock.h"

namespace sharkstore {
namespace test {
namespace mock {
MassTreeIteratorMock::MassTreeIteratorMock(std::unique_ptr<Scaner> scaner, uint64_t version,
        const Releaser& release_func, bool seek)
        : MassTreeIterator(std::move(scaner), version, release_func, seek)
{
    if (!seek && scaner_->Valid()) {
        cur_key_.from_string(scaner_->Key());
    }
}


void MassTreeIteratorMock::Traverse() {
    scaner_->Next();
    if (scaner_->Valid()) {
        cur_key_.from_string(scaner_->Key());
    } else {
        MultiVersionKey tmp;
        cur_key_ = tmp;
    }
}

MultiVersionKey MassTreeIteratorMock::getMultiKey() {
    return cur_key_;
}
}
}
}
