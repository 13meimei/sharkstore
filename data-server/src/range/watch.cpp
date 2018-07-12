//#include "watch.h"
#include <common/socket_session.h>
#include "watch.hpp"
#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {
void Range::AddKeyWatcher(std::string name, common::ProtoMessage* msg) {
    return key_watchers_.AddWatcher(name, msg);
}

WATCH_CODE Range::DelKeyWatcher(int64_t id) {
    return key_watchers_.DelWatcher(id);
}

WATCH_CODE Range::GetKeyWatchers(std::vector<common::ProtoMessage*>& vec, std::string name) {
    return key_watchers_.GetWatchers(vec, name);
}

// encode keys into buffer
void EncodeWatchKey(std::string* buf, uint64_t tableId, std::vector<std::string*> keys) {
    assert(buf != nullptr && buf->length() == 0);
    assert(keys.size() != 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    for (auto key : keys) {
        EncodeBytesAscending(buf, key->c_str(), key->length());
    }
}

// decode buffer to keys
bool DecodeWatchKey(std::vector<std::string*>& keys, std::string* buf) {
    assert(keys.size() == 0 && buf->length() > 9);

    size_t offset;
    for (offset = 9; offset != buf->length()-1; ) {
        auto b = new std::string;
        if (!DecodeBytesAscending(*buf, offset, b)) {
            return false;
        }
        keys.push_back(b);
    }
    return true;
}

void EncodeWatchValue(std::string* buf,
                              int64_t version,
                              const std::string* value,
                              const std::string* extend) {
    assert(buf != nullptr);
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value->c_str(), value->length());
    EncodeBytesValue(buf, 4, extend->c_str(), extend->length());
}

bool DecodeWatchValue(int64_t* version, std::string* value, std::string* extend,
                      std::string& buf) {
    assert(version != nullptr && value != nullptr && extend != nullptr &&
           buf.length() != 0);

    size_t offset = 0;
    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, value)) return false;
    if (!DecodeBytesValue(buf, offset, extend)) return false;
    return true;
}

}
}
}
