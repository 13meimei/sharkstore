_Pragma("once");

#include <vector>

#include "mass_types.h"
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
    ~Scaner() {
        if (rcu_stop_) {
            rcu_stop_();
        }
    }

    using RcuStoper = std::function<void()>;

    Scaner(TreeType* tree, const std::string vbegin, const std::string vend, const RcuStoper& rcu_stop):
            tree_(tree), vbegin_(vbegin), vend_(vend), last_key_(vbegin), rcu_stop_(rcu_stop) {
        do_scan();
    }
    Scaner(TreeType* tree, const std::string vbegin, const std::string vend, size_t max_rows, const RcuStoper& rcu_stop):
            tree_(tree), vbegin_(vbegin), vend_(vend), max_rows_(max_rows), last_key_(vbegin), rcu_stop_(rcu_stop) {
        do_scan();
    }

    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {
    }

    bool visit_value(Str key, std::string* value, threadinfo &) {
//        std::cout << "visit value: key " << std::string(key.s, key.len) << " value: " << *value << std::endl;
        auto k = std::string(key.data(), key.length());
        if (rows_ == max_rows_) {
            last_key_ = k;
            last_flag_ = true;
            return false;
        }

        if ((!vend_.empty()) && (k >= vend_)) {
            last_key_ = k;
            last_flag_ = true;
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
                if (last_flag_) {
                    do_scan();
                }
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
        if (!last_flag_) {
            return false;
        }
        if (kvs_.size() != max_rows_) {
            return false;
        } else {
            if (vend_.empty()) {
                return true;
            } else {
                return last_key_ < vend_;
            }
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
        last_flag_ = false;
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

    bool last_flag_;
    std::string last_key_;
    KVPairVector kvs_;
    KVPairVector::iterator kvs_it_;
    KVPair it_kv_;

    RcuStoper rcu_stop_;
};

}
}
}
