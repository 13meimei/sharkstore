#ifndef __SHARKSTORE_DS_MVCC_H__
#define __SHARKSTORE_DS_MVCC_H__

#include <atomic>
#include "tbb/concurrent_hash_map.h"

namespace sharkstore { namespace test { namespace mock { class MvccMock; }}}

namespace sharkstore {
namespace dataserver {
namespace storage {

class Mvcc {
public:
    friend class sharkstore::test::mock::MvccMock;
    Mvcc() = default;
    ~Mvcc() = default;
    Mvcc(const Mvcc&) = delete;
    Mvcc &operator=(const Mvcc&) = delete;

public:
    bool insert(uint64_t ver) {
        tbb::concurrent_hash_map<uint64_t, int>::accessor a;
        return mvcc_.insert(a, ver);
    }

    uint64_t insert() {
        tbb::concurrent_hash_map<uint64_t, int>::accessor a;
        auto ret = ++version_;
        mvcc_.insert(a, ret);
        return ret;
    }

    bool erase(uint64_t ver) {
        return mvcc_.erase(ver);
    }

    uint64_t min_ver() {
        uint64_t ver = UINT64_MAX;
        for (auto it = mvcc_.begin(); it != mvcc_.end(); ++it) {
            if (it->first < ver) {
                ver = it->first;
            }
        }
        return ver;
    }

    uint64_t load() {
        return version_;
    }

private:
    std::atomic<uint64_t> version_ = {0};
    //todo::init appropriate capacity
    tbb::concurrent_hash_map<uint64_t, int> mvcc_;
};
}
}
}
#endif //__SHARKSTORE_DS_MVCC_H__
