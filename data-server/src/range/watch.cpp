#include <common/socket_session.h>
#include <common/socket_session_impl.h>
#include "watch.h"
#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {
int32_t Range::AddKeyWatcher(std::string &name, common::ProtoMessage *msg) {
    return key_watchers_.AddWatcher(name, msg);
}

WATCH_CODE Range::DelKeyWatcher(int64_t &id, std::string &key) {
    return key_watchers_.DelWatcher(id, key);
}

uint32_t Range::GetKeyWatchers(std::vector<common::ProtoMessage *> &vec, std::string name) {
    return key_watchers_.GetWatchers(vec, name);
}

bool Watcher::operator<(const Watcher& other) const {
    return msg_->expire_time > other.msg_->expire_time;
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
        std::string* b = new std::string();
        
        if (!DecodeBytesAscending(*buf, offset, b)) {
            return false;
        }
        keys.push_back(b);
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

WatcherSet::WatcherSet() {
    watchers_expire_thread_ = std::thread([this]() {
        while (watchers_expire_thread_continue_flag) {
            Watcher w;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (timer_.empty()) {
                    timer_cond_.wait_for(lock, std::chrono::milliseconds(10));
                    continue; // sleep 10ms
                }

                w = timer_.top();
                auto milli = std::chrono::milliseconds(w.msg_->expire_time);
                std::chrono::system_clock::time_point expire( milli);

                std::cout << "wait_until> expire:" << milli.count() << "    now:" << getticks() << std::endl;

                if (timer_cond_.wait_until(lock, expire) == // todo
                    std::cv_status::timeout) {

                    FLOG_DEBUG("thread send timeout response.");
                    // send timeout response
                    auto resp = new watchpb::DsWatchResponse;
                    resp->mutable_resp()->set_code(Status::kTimedOut);
                    common::SocketSessionImpl session;

                    FLOG_DEBUG("session_id:%" PRIu64 " msg_id:%" PRIu64 " test.", w.msg_->session_id, w.msg_->msg_id);
                    session.Send(w.msg_, resp);

                    // delete watcher
                    {
                        auto id = w.msg_->session_id;
                        auto key = w.key_;
                        auto wit = watcher_index_.find(id);
                        if (wit == watcher_index_.end()) {
                            continue;
                        }

                        auto keys = wit->second; // key map
                        auto itKeys = keys->find(key);

                        if (itKeys != keys->end()) {
                            auto itKeyIdx = key_index_.find(key);

                            if (itKeyIdx != key_index_.end()) {
                                auto itSession = itKeyIdx->second->find(id);

                                if (itSession != itKeyIdx->second->end()) {
                                    itKeyIdx->second->erase(itSession);
                                }
                                key_index_.erase(itKeyIdx);
                            }

                            keys->erase(itKeys);
                        }

                        if (keys->size() < 1) {
                            watcher_index_.erase(wit);
                        }
                    } // del watcher

                    timer_.pop();
                }
            }
        }
    });
}

WatcherSet::~WatcherSet() {
    watchers_expire_thread_continue_flag = false;
    watchers_expire_thread_.join();

    //to do release pointer
    for(auto it:key_index_) {
        free(it.second);
    }
    //to do release pointer
    for(auto it:watcher_index_) {
        free(it.second);
    }
}

int32_t WatcherSet::AddWatcher(std::string &name, common::ProtoMessage *msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();

    if(key_index_.size() >= MAX_WATCHER_SIZE) {
        FLOG_ERROR("AddWatcher fail:exceed max size(%d) of watcher list", MAX_WATCHER_SIZE);
        return -1;
    }
    //std::shared_ptr<common::ProtoMessage> msgPtr = std::make_shared<common::protoMessage>(*msg);
    timer_.push(Watcher(msg, name));
    
    // build key name to watcher session id
    auto kit0 = key_index_.find(name);
    if (kit0 == key_index_.end()) {
        auto val = new WatcherSet_;
        val->emplace(std::make_pair(msg->session_id, msg));

        auto tmpPair = key_index_.emplace(name, val);

        if (tmpPair.second) kit0 = tmpPair.first;
        else FLOG_DEBUG("AddWatcher abnormal key:%s session_id:%" PRId64, name.data(), msg->session_id);
    }

    if (kit0 != key_index_.end()) {
        auto kit1 = kit0->second->find(msg->session_id);
        if (kit1 == kit0->second->end()) {
            kit0->second->emplace(std::make_pair(msg->session_id, msg));
        }
        // build watcher id to key name
        auto wit0 = watcher_index_.find(msg->session_id);
        if (wit0 == watcher_index_.end()) {
            auto val = new KeySet_;
            val->emplace(name, 1);

            auto tmpPair = watcher_index_.emplace(std::make_pair(msg->session_id, val));
            if (tmpPair.second) wit0 = tmpPair.first;
            else FLOG_DEBUG("AddWatcher abnormal session_id:%" PRId64 " key:%s .", msg->session_id, name.data());
        }

        if (wit0 != watcher_index_.end()) {
            auto wit1 = wit0->second->find(name);
            if (wit1 == wit0->second->end()) {
                wit0->second->emplace(std::make_pair(name, 2));
            }
        }
    }

    return 0;
}

WATCH_CODE WatcherSet::DelWatcher(int64_t id, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();
    //  timer_.find and del
    auto timer_queue = timer_.GetQueue();
    for (auto it = timer_queue.begin(); it != timer_queue.end(); ++it) {
        if (it->msg_->session_id == id && it->key_ == key) {
            timer_.GetQueue().erase(it);
        }
    }

    //<session:keys>
    int64_t tmpSessionId(id);
    auto wit = watcher_index_.find(tmpSessionId);
    if (wit == watcher_index_.end()) {
        std::cout << "return fail" << std::endl;
        return WATCH_WATCHER_NOT_EXIST;
    }
    //KeySet_*
    auto &keys = wit->second; // key map
    auto itKeys = keys->find(key);

    if (itKeys != keys->end()) {
        std::cout << "find key:" << key << std::endl;
        auto itKeyIdx = key_index_.find(key);

        if (itKeyIdx != key_index_.end()) {
            auto itSession = itKeyIdx->second->find(std::move(id));

            if (itSession != itKeyIdx->second->end()) {
                itKeyIdx->second->erase(itSession);
            }
            key_index_.erase(itKeyIdx);
        }

        keys->erase(itKeys);
    }

    if (keys->size() < 1) {
        watcher_index_.erase(wit);
    }

    return WATCH_OK;
}

uint32_t WatcherSet::GetWatchers(std::vector<common::ProtoMessage *> &vec, std::string &name) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();

    auto kit = key_index_.find(name);
    if (kit == key_index_.end()) {
        return WATCH_KEY_NOT_EXIST;
    }

    auto watchers = kit->second;
    uint32_t msgSize = static_cast<uint32_t>(watchers->size());

    if (msgSize > 0) {
        vec.resize(msgSize);

        for (auto it = watchers->begin(); it != watchers->end(); ++it) {
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

                    FLOG_DEBUG("range[%" PRIu64"] EncodeKv:(%s) key%d):%s", meta_.id(), funcName.data(), i, kv->mutable_key(i)->data());
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
                FLOG_DEBUG("range[%" PRIu64 "] %s info: table_id:%" PRId64" key before:%s after:%s",
                           meta_.id(), funcName.data(), meta_.table_id(),  keys[0]->data(), EncodeToHexString(db_key).c_str());

                if (!kv->value().empty()) {
                    int64_t tmpVersion = kv->version();
                    EncodeWatchValue( &db_value, tmpVersion, kv->mutable_value(), &ext);

                    FLOG_DEBUG("range[%" PRIu64 "] %s info: value before:%s after:%s", 
                            meta_.id(), funcName.data(), kv->value().data(), EncodeToHexString(db_value).c_str());
                }
                break;
            
            default:
                ret = -1;
                err->set_message("unknown func_id");
                FLOG_WARN("range[%" PRIu64 "] error: unknown func_id:%d", meta_.id(), funcId);
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
                
                FLOG_WARN("range[%" PRIu64 "] version(decode from value)[%" PRIu64 "]", meta_.id(), version);

                if(kv->key_size() <= 0)
                    kv->add_key(db_key);
                //kv->set_key(0, db_key);
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
                FLOG_WARN("range[%" PRIu64 "] error: unknown func_id:%d", meta_.id(), funcId);
                break;
        }
        return ret;
}

int16_t  WatchCode::NextComparableBytes(const char *key, const int16_t len, std::string &result) {
    if(len > 0) {
        result.resize(len);
    }

    for( auto i = len - 1; i >= 0; i--) {

        uint8_t keyNo = static_cast<uint8_t>(key[i]);

        if( keyNo < 0xff) {
            keyNo++;
            //*result = key;
            result[i] = static_cast<char>(keyNo);

            return 0;
        }
    }

    return -1;
}

} //end namespace range
} //end namespace data-server
} //end namespace sharkstore

