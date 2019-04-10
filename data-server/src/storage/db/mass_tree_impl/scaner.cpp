#include "scaner.h"

#include "mass_tree_db.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Scaner::Scaner(MassTreeDB* tree, const std::string& vbegin, const std::string& vend, size_t max_rows):
    tree_(tree), vbegin_(vbegin), vend_(vend), max_rows_(max_rows), last_key_(vbegin) {
    do_scan();
}

bool Scaner::visit_value(Masstree::Str key, std::string* value, threadinfo &) {
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

bool Scaner::Valid() {
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

void Scaner::Next() {
    kvs_it_++;
}

bool Scaner::scan_valid() {
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

bool Scaner::iter_valid() {
    return kvs_it_ != kvs_.end();
}

void Scaner::do_scan() {
    reset();

    tree_->Scan(last_key_, *this);

    kvs_it_ = kvs_.begin();
}

void Scaner::reset() {
    rows_ = 0;
    last_flag_ = false;
    kvs_.clear();
}

}
}
}
