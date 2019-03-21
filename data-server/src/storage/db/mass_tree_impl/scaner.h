_Pragma("once");

#include <vector>
#include "masstree-beta/masstree_tcursor.hh"
#include "masstree-beta/masstree_struct.hh"
#include "masstree-beta/str.hh"
#include "masstree-beta/kvrow.hh"
#include "mass_tree_impl.h"
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

    Scaner(TreeType* tree, std::unique_ptr<threadinfo, ThreadInfoDeleter>& thread_info,
           const std::string vbegin, const std::string vend):
            tree_(tree), thread_info_(thread_info),
           vbegin_(vbegin), last_key_(vbegin), vend_(vend) {
    }
    Scaner(TreeType* tree, std::unique_ptr<threadinfo, ThreadInfoDeleter>& thread_info,
           const std::string vbegin, const std::string vend, size_t rows):
            tree_(tree), thread_info_(thread_info),
            vbegin_(vbegin), last_key_(vbegin), vend_(vend), rows_(rows) {
    }

    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {
    }

    bool visit_value(Str key, std::string* value, threadinfo &) {
        std::cout << "visit value: key " << std::string(key.s, key.len) << " value: " << value << std::endl;
        auto k = std::string(key.data(), key.length());
        if (k >= vend_ || rows_ > max_rows_) {
            last_key_ = k;
            return false;
        }

        auto kv = std::make_pair(k, *value);
        kvs_.push_back(kv);
        rows_++;
        return true;
    }

    bool Valid() {
        auto scan_valid = (last_key_ < vend_ && rows_ > 0);
        auto iter_valid = (kvs_it_ != kvs_.end());

        if (scan_valid) {
            if (iter_valid) {
                return true;
            } else {
                do_scan();
            }
        }

        if (iter_valid) {
            return true;
        } else {
            return false;
        }
    }

    const KVPair&& Next() {
        auto k = kvs_it_->first;
        auto v = kvs_it_->second;
        kvs_it_++;
        return std::move(KVPair(k, v));
    }

private:
    void do_scan() {
        kvs_.clear();
        while (scan<TreeType>(*tree_, *thread_info_.get())) {
        }
        kvs_it_ = kvs_.begin();
    }

private:
    template<typename T>
    int scan(T& table, threadinfo &ti) {
        return table.scan(Str(last_key_.c_str(), last_key_.length()), true, *this, ti);
    }

    template<typename T>
    int rscan(T& table, threadinfo &ti) {
        return table.rscan(Str(last_key_.c_str(), last_key_.length()), true, *this, ti);
    }

private:
    const std::string vbegin_;
    const std::string vend_;
    const size_t max_rows_ = 100;
    size_t rows_ = 0;

    std::string last_key_;
    KVPairVector kvs_;
    KVPairVector::iterator kvs_it_;

    TreeType* tree_;
    std::unique_ptr<threadinfo, ThreadInfoDeleter>& thread_info_;
};

}
}
}
