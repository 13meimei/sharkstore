_Pragma("once");

#include <string>

#include "sl_map.h"
#include "iterator.h"

namespace memstore {

template<typename V>
class Store {
public:
    Store() = default;
    ~Store() = default;

    Store(const Store &) = delete;
    Store &operator=(const Store &) = delete;

    int Get(const std::string &key, V* value) {
        auto iter = slist_.find(key);
        if (iter == slist_.end()) {
            return -1;
        }

        *value = iter->second;
        return 0;
    }

    int Put(const std::string &key, const V& value) {
        auto ret = slist_.upsert(std::make_pair(key, value));
        if (ret.second == false) {
            return -1;
        }

        return 0;
    }

    int Delete(const std::string &key) {
        // todo erase always return 0
        return static_cast<int>(slist_.erase(key));
    }

    int DeleteRange(const std::string& start_key, const std::string& end_key) {
        if (start_key > end_key) {
            return -1;
        }

        auto it = slist_.find(start_key);
        if (it == slist_.end()) {
            return -1;
        }

        auto i = 0;
        while ((it != slist_.end()) && (it->first < end_key)) {
            it = slist_.erase(it);
            i++;
        }
        return i;
    }

    map_iterator<std::string, V> Seek(const std::string& start) {
        return slist_.find_greater_or_equal(start);
    };

    Iterator<std::string, V>* NewIterator(std::string start, std::string limit) {
        if (start.empty() || limit.empty()) {
            return nullptr;
        }
        if (start > limit) {
            return nullptr;
        }

        return new Iterator<std::string, V>(Seek(start), start, limit);
    };

private:
    sl_map_gc<std::string, V> slist_;
};

}
