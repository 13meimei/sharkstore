_Pragma("once");

#include "sl_map.h"

namespace memstore {

template <typename K, typename V>
class Iterator {
public:
    Iterator() = delete;
    Iterator(Iterator& iter) = delete;
    Iterator& operator=(Iterator iter) = delete;

    Iterator(map_iterator<K, V> iter,
             const std::string& start, const std::string& limit):
            iter_(std::move(iter)) {
        if (start > limit) {
            return;
        }

        limit_ = limit;
    }

    ~Iterator() {}

    void Next() {
        ++iter_;
        if (iter_.Valid()) {
            if (iter_->first >= limit_) {
                iter_.SetInvalid();
            }
        }
    }

    bool Valid() {
        return iter_.Valid();
    }

    const K& Key() {
        return iter_->first;
    }

    const V& Value() {
        return iter_->second;
    }

private:
    map_iterator<K, V> iter_;
    std::string limit_;
};

}
