#ifndef SHARKSTORE_MULTIVERSIONKEY_H
#define SHARKSTORE_MULTIVERSIONKEY_H
#include <string>
#include <cstdint>

namespace sharkstore {
namespace dataserver {
namespace storage {

class MultiVersionKey {
public:
    MultiVersionKey() = default;
    MultiVersionKey(std::string key, uint64_t version, bool flag = false)
        :key_(key),version_(version), del_flag_(flag) {};
    MultiVersionKey(const MultiVersionKey& key)
        :key_(key.key_),version_(key.version_) {};
    ~MultiVersionKey() = default;

public:
    void set_key(const std::string& key) {
        key_ = key;
    }

    void set_ver(const uint64_t version) {
        version_ = version;
    }

    std::string key() const {
        return key_;
    }

    uint64_t ver() const {
        return version_;
    }

    bool is_del() const {
        return del_flag_;
    }

    uint64_t size() const {
        return key_.length() + sizeof(version_);
    }

    bool empty() const {
        return key_.empty();
    }

    bool operator==(const MultiVersionKey& key) const {
        return key_ == key.key_ && version_ == key.version_;
    }

    bool operator!=(const MultiVersionKey& key) const {
        return key_ != key.key_ || version_ != key.version_;
    }

    bool operator>(const MultiVersionKey& key) const {
        if(key_ > key.key_) {
            return true;
        } else if(key_ == key.key_ && version_ < key.version_) {
            return true;
        }
        return false;
    }

    bool operator<(const MultiVersionKey& key) const {
        if(key_ < key.key_) {
            return true;
        } else if (key_ == key.key_ && version_ > key.version_) {
            return true;
        }
        return false;
    }

    bool operator>=(const MultiVersionKey& key) const {
        if(key_ > key.key_) {
            return true;
        } else if(key_ == key.key_ && version_ <= key.version_) {
            return true;
        }
        return false;
    }

    bool operator<=(const MultiVersionKey& key) const {
        if(key_ < key.key_) {
            return true;
        } else if (key_ == key.key_ && version_ >= key.version_) {
            return true;
        }
        return false;
    }

    MultiVersionKey& operator=(const MultiVersionKey& key) {
        if(this != &key) {
            key_ = key.key_;
            version_ = key.version_;
        }
        return *this;
    }
private:
    std::string key_;
    uint64_t version_;
    bool     del_flag_;
}; // end MultiVersionKey

}
}
}
#endif //SHARKSTORE_MULTIVERSIONKEY_H
