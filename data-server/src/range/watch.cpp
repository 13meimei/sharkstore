//#include "watch.h"
#include <common/socket_session.h>
#include "watch.h"
#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {
void Range::AddKeyWatcher(std::string &name, common::ProtoMessage *msg) {
    key_watchers_.AddWatcher(name, msg);
}

WATCH_CODE Range::DelKeyWatcher(const int64_t &id, const std::string &key) {
    return key_watchers_.DelWatcher(id, key);
}

uint32_t Range::GetKeyWatchers(std::vector<common::ProtoMessage *> &vec, std::string name) {
    return key_watchers_.GetWatchers(vec, name);
}

// encode keys into buffer
void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys) {
    assert(buf != nullptr && buf->length() != 0);
    assert(keys.size() != 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    for (auto key : keys) {
        EncodeBytesAscending(buf, key->c_str(), key->length());
    }
}

// decode buffer to keys
bool DecodeWatchKey(std::vector<std::string *> &keys, std::string *buf) {
    assert(keys.size() == 0 && buf->length() > 9);

    size_t offset;
    for (offset = 9; offset != buf->length() - 1;) {
        auto b = std::make_shared<std::string>();

        if (!DecodeBytesAscending(*buf, offset, b.get())) {
            return false;
        }
        keys.push_back(b.get());
    }
    return true;
}
void EncodeWatchValue(std::string *buf,
                      int64_t &version,
                      const std::string *value,
                      const std::string *extend) {
    assert(buf != nullptr);
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value->c_str(), value->length());
    EncodeBytesValue(buf, 4, extend->c_str(), extend->length());
}

bool DecodeWatchValue(int64_t *version, std::string *value, std::string *extend,
                      std::string &buf) {
    assert(version != nullptr && value != nullptr && extend != nullptr &&
           buf.length() != 0);

    size_t offset = 0;
    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, value)) return false;
    if (!DecodeBytesValue(buf, offset, extend)) return false;
    return true;
}

void WatcherSet::AddWatcher(std::string &name, common::ProtoMessage *msg) {
    std::lock_guard<std::mutex> lock(mutex_);

    // build key name to watcher session id
    auto kit0 = key_index_.find(name);
    if (kit0 == key_index_.end()) {
        auto tmpPair = key_index_.emplace(std::make_pair(name, std::make_pair(msg->session_id, msg)));
        if (tmpPair.second) kit0 = tmpPair.first;
        else FLOG_DEBUG("AddWatcher abnormal key:%s session_id:%lld .", name.data(), msg->session_id);
    }

    if (kit0 != key_index_.end()) {
        auto kit1 = kit0->second.find(msg->session_id);
        if (kit1 == kit0->second.end()) {
            kit0->second.emplace(std::make_pair(msg->session_id, msg));
        }
        // build watcher id to key name
        auto wit0 = watcher_index_.find(msg->session_id);
        if (wit0 == watcher_index_.end()) {
            auto tmpPair = watcher_index_.emplace(std::make_pair(msg->session_id, std::make_pair(name, nullptr)));
            if (tmpPair.second) wit0 = tmpPair.first;
            else FLOG_DEBUG("AddWatcher abnormal session_id:%lld key:%s .", msg->session_id, name.data());
        }

        if (wit0 != watcher_index_.end()) {
            auto wit1 = wit0->second.find(name);
            if (wit1 == wit0->second.end()) {
                wit0->second.emplace(std::make_pair(name, nullptr));
            }
        }
    }

    return;
}

WATCH_CODE WatcherSet::DelWatcher(const int64_t &id, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);

    //<session:keys>
    auto wit = watcher_index_.find(id);
    if (wit == watcher_index_.end()) {
        return WATCH_WATCHER_NOT_EXIST;
    }

    auto keys = wit->second; // key map
    auto itKeys = keys.find(key);

    if (itKeys != keys.end()) {
        auto itKeyIdx = key_index_.find(key);

        if (itKeyIdx != key_index_.end()) {
            auto itSession = itKeyIdx->second.find(id);

            if (itSession != itKeyIdx->second.end()) {
                itKeyIdx->second.erase(itSession);
            }
            key_index_.erase(itKeyIdx);
        }

        keys.erase(itKeys);
    }

    if (keys.size() < 1) {
        watcher_index_.erase(wit);
    }

    return WATCH_OK;
}

uint32_t WatcherSet::GetWatchers(std::vector<common::ProtoMessage *> &vec, const std::string &name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto kit = key_index_.find(name);
    if (kit == key_index_.end()) {
        return WATCH_KEY_NOT_EXIST;
    }

    auto watchers = kit->second;
    uint32_t msgSize = static_cast<uint32_t>(watchers.size());

    if (msgSize > 0) {
        vec.resize(msgSize);

        for (auto it = watchers.begin(); it != watchers.end(); ++it) {
            vec.emplace_back(it->second);
        }
    }

    return msgSize;
}

int16_t WatchCode::EncodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv, 
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err) {
        int16_t ret(0);
        std::vector<std::string*> keys;
        std::string funcName("");
        std::string ext("");
        keys.clear();
        db_key.clear();

        switch (funcId) {
            case funcpb::kFuncWatchGet:
            case funcpb::kFuncPureGet:
            case funcpb::kFuncWatchPut:
            case funcpb::kFuncWatchDel:
                if (funcId == funcpb::kFuncWatchGet) {
                    funcName.assign("WatchGet");
                } else if (funcId == funcpb::kFuncPureGet) {
                    funcName.assign("PureGet");
                } else if (funcId == funcpb::kFuncWatchPut) {
                    funcName.assign("WatchPut");
                } else {
                    funcName.assign("WatchDel");
                }

                for (auto i = 0; i < kv->key_size(); i++) {
                    keys.push_back(kv->mutable_key(i));

                    FLOG_DEBUG("range[%"PRIu64"] %s key%d):%s", meta_.id(), funcName.data(), i, kv->mutable_key(i)->data());
                }

                if(kv->key_size()) {
                    EncodeWatchKey(&db_key, meta_.table_id(), keys);
                } else {
                    ret = -1;
                    if (db_key.empty() || kv->key_size() < 1) {
                        FLOG_WARN("range[%" PRIu64 "] %s error: key empty", meta_.id(), funcName.data());
                        //err = errorpb::KeyNotInRange(db_key);
                        break;
                    }
                }
                FLOG_DEBUG("range[%" PRIu64 "] %s info: table_id:%lld key before:%s after:%s", 
                           meta_.id(), funcName.data(), meta_.table_id(),  keys[0]->data(), db_key.data());

                if (!kv->value().empty()) {
                    int64_t tmpVersion = kv->version();
                    EncodeWatchValue( &db_value, tmpVersion, kv->mutable_value(), &ext);
                }
                FLOG_DEBUG("range[%" PRIu64 "] %s info: value before:%s after:%s", 
                           meta_.id(), funcName.data(), kv->value().data(), db_value.data());
                break;
            
            default:
                ret = -1;
                err->set_message("unknown func_id");
                FLOG_WARN("range[%" PRIu64 "] %s error: unknown func_id:%d", meta_.id(), funcId);
                break;
        }
        return ret;
    }

int16_t WatchCode::DecodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv, 
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err) {
        int16_t ret(0);
        std::vector<std::string*> keys;
        int64_t version(0);
        keys.clear();
        
        auto val = std::make_shared<std::string>("");
        auto ext = std::make_shared<std::string>("");

        switch (funcId) {
            case funcpb::kFuncWatchGet:
            case funcpb::kFuncPureGet:
            case funcpb::kFuncWatchPut:
            case funcpb::kFuncWatchDel:
                //decode value
                DecodeWatchValue(&version, val.get(), ext.get(), db_value);
                kv->set_key(0, db_key);
                kv->set_value(*val);
                kv->set_version(version);
                kv->set_ext(*ext);
                break;
            
            default:
                ret = -1;
                if (err == nullptr) {
                    err = new errorpb::Error;
                }
                err->set_message("unknown func_id");
                FLOG_WARN("range[%" PRIu64 "] %s error: unknown func_id:%d", meta_.id(), funcId);
                break;
        }
        return ret;
}

} //end namespace range
} //end namespace data-server
} //end namespace sharkstore

