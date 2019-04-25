#ifndef SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H
#define SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H

#include <memory>
#include <functional>
#include "storage/db/mass_tree_impl/iterator.h"
#include "mass_tree_mvcc_mock.h"
#include "storage/db/mass_tree_impl/scaner.h"

namespace sharkstore {
namespace test {
namespace mock {

using namespace sharkstore::dataserver::storage;

class MassTreeIteratorMock: public MassTreeIterator{
public:
    MassTreeIteratorMock(std::unique_ptr<Scaner> scaner, uint64_t version,
            const Releaser& release_func, bool seek = false);

    ~MassTreeIteratorMock() = default;

    void Traverse() ;

    MultiVersionKey getMultiKey() ;
};

}
}
}

#endif //SHARKSTORE_DS_MASS_TREE_ITERATOR_MOCK_H
