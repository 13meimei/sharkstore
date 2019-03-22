#ifndef SHARKSTORE_MULTIVERSIONKEY_H
#define SHARKSTORE_MULTIVERSIONKEY_H
#include <string>
#include <cstdint>

#include "common/ds_encoding.h"

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
    std::string to_string() const {
        std::string buf;
        EncodeBytesAscending(&buf, key_.c_str(), key_.size());
        auto version_tag = (version_ << 8); // 高7位是version，低1位是标记
        if (!del_flag_) { //
            version_tag |= 1; // put的标记为1（默认是put即del_flag_为false，保持默认情况下的key在前面)
        }
        EncodeUvarintDescending(&buf, version_tag);
        return buf;
    }

    bool from_string(std::string &key) {
        size_t pos = 0;
        if (!DecodeBytesAscending(key, pos, &key_)) {
            return false;
        }
        uint64_t version_tag = 0;
        if (!DecodeUvarintDescending(key, pos, &version_tag)) {
            return false;
        }
        version_ = version_tag >> 8;
        if (static_cast<uint8_t>(version_tag & 0xff) == 0) {
            del_flag_ = true;
        }
        return true;
    }

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
