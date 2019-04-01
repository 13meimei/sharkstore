#ifndef SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H
#define SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H

#define private public

#include <memory>
#include <functional>
#include "storage/db/mass_tree_impl/iterator.h"
#include "mass_tree_mvcc_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

class Scaner;
class MvccMassTreeMock;

using namespace sharkstore::dataserver::storage;

class MassTreeIteratorMock: public MassTreeIterator{
public:
using Releaser = std::function<void()>;

    MassTreeIteratorMock(std::unique_ptr<Scaner> scaner, uint64_t version, const Releaser& release_func);
    ~MassTreeIteratorMock();

    void Traverse() {
        scaner_->Next();
    }
};

}
}
}

#endif //SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H
