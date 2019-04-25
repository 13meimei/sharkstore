#ifndef SHARKSTORE_DS_MASS_TREE_MVCC_MOCK_H
#define SHARKSTORE_DS_MASS_TREE_MVCC_MOCK_H

#include "storage/db/mass_tree_impl/mass_tree_mvcc.h"
//#include "mvcc_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

using namespace sharkstore::dataserver::storage;

class MvccMassTreeMock : public MvccMassTree {
public:
    friend class MassTreeIteratorMock;

    MvccMassTreeMock() = default;
    ~MvccMassTreeMock() = default;

    IteratorInterface* newIterMock(MvccTree *tree,
            const std::string &start, const std::string &limit);

    IteratorInterface* NewIterator(const std::string& start, const std::string& limit) {
        return newIterMock(&default_tree_, start, limit);
    }

    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
            std::unique_ptr<IteratorInterface>& txn_iter,
            const std::string& start, const std::string& limit)
    {
        data_iter.reset(newIterMock(&default_tree_, start, limit));
        txn_iter.reset(newIterMock(&txn_tree_, start, limit));
        return Status::OK();
    }

    uint64_t LoadVersion() { return default_tree_.mvcc.load(); }
    void StoreVersion(uint64_t ver) { default_tree_.mvcc.store(ver); }

    void seek_set(bool seek) {
         seek_ = seek;
    }

    void Scrub() {
        MvccMassTree::scrub(static_cast<MvccMassTree::MvccTree*>(DefaultColumnFamily()));
    }

private:
    //MvccMock mvcc_;
    bool seek_ = true;
};

}
}
}


#endif //SHARKSTORE_DS_MASS_TREE_MVCC_MOCK_H
