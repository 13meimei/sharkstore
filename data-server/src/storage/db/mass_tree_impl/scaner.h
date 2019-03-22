_Pragma("once");

#include <vector>
#include "mass_tree_impl.h"
#include "masstree-beta/masstree_tcursor.hh"
#include "masstree-beta/masstree_struct.hh"
#include "masstree-beta/str.hh"
#include "masstree-beta/masstree_scan.hh"

namespace sharkstore {
namespace dataserver {
namespace storage {

using namespace lcdf;
using KVPair = std::pair<std::string, std::string>;
using KVPairVector = std::vector<std::pair<std::string, std::string>>;

class Scaner {
public:
    Scaner() = delete;
    ~Scaner() = default;

    Scaner(TreeType* tree, const std::string vbegin, const std::string vend):
            tree_(tree), vbegin_(vbegin), vend_(vend), last_key_(vbegin) {
        do_scan();
    }
    Scaner(TreeType* tree, const std::string vbegin, const std::string vend, size_t max_rows):
            tree_(tree), vbegin_(vbegin), vend_(vend), max_rows_(max_rows), last_key_(vbegin) {
        do_scan();
    }

    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {
    }

    bool visit_value(Str key, std::string* value, threadinfo &) {
//        std::cout << "visit value: key " << std::string(key.s, key.len) << " value: " << *value << std::endl;
        auto k = std::string(key.data(), key.length());

        if (rows_ > max_rows_) {
            last_key_ = k;
            return false;
        }

        if (!vend_.empty() && k >= vend_) {
            last_key_ = k;
            return false;
        }

        auto kv = std::make_pair(k, *value);
        kvs_.push_back(kv);
        rows_++;
        return true;
    }

    bool Valid() {
        if (scan_valid()) {
            if (iter_valid()) {
                goto ITER;
            } else {
                do_scan();
            }
        }

        ITER:
        if (iter_valid()) {
            it_kv_ = std::make_pair(kvs_it_->first, kvs_it_->second);
            return true;
        } else {
            return false;
        }
    }

    void Next() {
        kvs_it_++;
    }

    std::string Key() { return it_kv_.first; }
    std::string Value() { return it_kv_.second; }

private:
    bool scan_valid() {
        if (vend_.empty()) {
            return rows_ > 0 && rows_ > max_rows_;
        } else {
            return (last_key_ < vend_) && (rows_ > 0);
        }
    }

    bool iter_valid() {
        return kvs_it_ != kvs_.end();
    }

    void do_scan() {
        reset();

        thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> ti(threadinfo::make(threadinfo::TI_PROCESS, -1));
        scan<TreeType>(*tree_, *ti.get());

        kvs_it_ = kvs_.begin();
    }

    void reset() {
        rows_ = 0;
        kvs_.clear();
    }

private:
    template<typename T>
    int scan(T& table, threadinfo &ti) {
        return table.scan(Str(last_key_.c_str(), last_key_.length()), true, *this, ti);
    }

private:
    TreeType* tree_;

    const std::string vbegin_;
    const std::string vend_;
    const size_t max_rows_ = 100;
    size_t rows_ = 0;

    std::string last_key_;
    KVPairVector kvs_;
    KVPairVector::iterator kvs_it_;
    KVPair it_kv_;
};

}
}
}
