#ifndef SHARKSTORE_DS_MVCC_MOCK_H
#define SHARKSTORE_DS_MVCC_MOCK_H

#define private public

#include "storage/db/mvcc.h"
using namespace sharkstore::dataserver::storage;

class MvccMock : public Mvcc {
public:
    void store(uint64_t ver) {
        version_.store(ver);
    }
};
#endif //SHARKSTORE_DS_MVCC_MOCK_H
