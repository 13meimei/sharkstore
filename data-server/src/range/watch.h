_Pragma("once");
#include <string>
#include <proto/gen/watchpb.pb.h>
#include <proto/gen/funcpb.pb.h>
#include <proto/gen/errorpb.pb.h>
#include <common/socket_session.h>
#include <common/ds_encoding.h>
#include <frame/sf_logger.h>
#include <mutex>
#include <memory>

namespace sharkstore {
namespace dataserver {
namespace range {

class WatchCode {
public:
    WatchCode(){};
    ~WatchCode(){};

public:
    static int16_t EncodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv,
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

                    //FLOG_DEBUG("range[%"PRIu64"] %s key%d):%s", meta_.id(), funcName.data(), i, kv->mutable_key(i)->data());
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
                //FLOG_DEBUG("range[%" PRIu64 "] %s info: table_id:%lld key before:%s after:%s",
                //          meta_.id(), funcName.data(), meta_.table_id(),  keys[0]->data(), db_key.data());

                if (!kv->value().empty()) {
                    EncodeWatchValue(&db_value, kv->version(), &(kv->value()), &ext);
                }
//                FLOG_DEBUG("range[%" PRIu64 "] %s info: value before:%s after:%s",
//                           meta_.id(), funcName.data(), kv->value().data(), db_value.data());
                break;

            default:
                ret = -1;
                err->set_message("unknown func_id");
                //FLOG_WARN("range[%" PRIu64 "] %s error: unknown func_id:%d", meta_.id(), funcId);
                break;
        }
        return ret;
    }

    static int16_t DecodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv,
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
                //FLOG_WARN("range[%" PRIu64 "] %s error: unknown func_id:%d", meta_.id(), funcId);
                break;
        }
        return ret;
    }

};


//enum WATCH_CODE {
//    WATCH_OK = 0,
//    WATCH_KEY_NOT_EXIST,
//    WATCH_WATCHER_NOT_EXIST,
//};
//
//typedef std::map<int64_t, common::ProtoMessage*> WatcherSet_;
//typedef std::map<std::string, WatcherSet_*> Key2Watchers_;
//typedef std::map<std::string, nullptr_t> KeySet_;
//typedef std::map<int64_t, KeySet_*> Watcher2Keys_;
//
//class WatcherSet {
//public:
//    WatcherSet() {};
//    ~WatcherSet() {};
//    void AddWatcher(std::string, common::ProtoMessage*);
//    WATCH_CODE DelWatcher(int64_t);
//    WATCH_CODE GetWatchers(std::vector<common::ProtoMessage*>&, std::string);
//
//private:
//    Key2Watchers_ key_index_;
//    Watcher2Keys_ watcher_index_;
//    std::mutex mutex_;
//};
//
//
//void WatcherSet::AddWatcher(std::string name, common::ProtoMessage* msg) {
//    std::lock_guard<std::mutex> lock(mutex_);
//
//    // build key name to watcher session id
//    auto kit0 = key_index_.find(name);
//    if (kit0 == key_index_.end()) {
//        key_index_.insert(std::make_pair(name, new WatcherSet_));
//        kit0 = key_index_.find(name);
//    }
//    assert(kit0 != key_index_.end());
//
//    auto kit1 = kit0->second->find(msg->session_id);
//    if (kit1 == kit0->second->end()) {
//        kit0->second->insert(std::make_pair(msg->session_id, msg));
//    }
//
//    // build watcher id to key name
//    auto wit0 = watcher_index_.find(msg->session_id);
//    if (wit0 == watcher_index_.end()) {
//        watcher_index_.insert(std::make_pair(msg->session_id, new KeySet_));
//        wit0 = watcher_index_.find(msg->session_id);
//    }
//    assert(wit0 != watcher_index_.end());
//
//    auto wit1 = wit0->second->find(name);
//    if (wit1 == wit0->second->end()) {
//        wit0->second->insert(std::make_pair(name, nullptr));
//    }
//}
//
//WATCH_CODE WatcherSet::DelWatcher(int64_t id) {
//    std::lock_guard<std::mutex> lock(mutex_);
//
//    auto wit = watcher_index_.find(id);
//    if (wit == watcher_index_.end()) {
//        return WATCH_WATCHER_NOT_EXIST;
//    }
//
//    auto keys = wit->second; // key map
//    for (auto key = keys->begin(); key != keys->end(); ++key) {
//        auto kit = key_index_.find(key->first);
//        assert(kit != key_index_.end());
//        auto watchers = kit->second;
//        watchers->erase(id);
//    }
//    watcher_index_.erase(id);
//
//    return WATCH_OK;
//}
//
//WATCH_CODE WatcherSet::GetWatchers(std::vector<common::ProtoMessage*>& vec, std::string name) {
//    std::lock_guard<std::mutex> lock(mutex_);
//
//    auto kit = key_index_.find(name);
//    if (kit != key_index_.end()) {
//        return WATCH_KEY_NOT_EXIST;
//    }
//
//    auto watchers = kit->second;
//    for (auto it = watchers->begin(); it != watchers->end(); ++it) {
//        vec.push_back(it->second);
//    }
//    return WATCH_OK;
//}
//
//} // namespace end
//}
//}

enum WATCH_CODE {
    WATCH_OK = 0,
    WATCH_KEY_NOT_EXIST = -1,
    WATCH_WATCHER_NOT_EXIST = -2
};

typedef std::unordered_map<int64_t, common::ProtoMessage*> WatcherSet_;
typedef std::unordered_map<std::string, WatcherSet_> Key2Watchers_;
typedef std::unordered_map<std::string, nullptr_t> KeySet_;
typedef std::unordered_map<int64_t, KeySet_> Watcher2Keys_;

struct Watcher{
    Watcher() = delete;
    Watcher(common::ProtoMessage* msg, std::string* key) {
        this->msg_ = msg;
        this->key_ = key;
    }
    ~Watcher() = default;
    bool operator<(const Watcher& other) const;

    common::ProtoMessage* msg_;
    std::string* key_;
};

bool Watcher::operator<(const Watcher& other) const {
    return msg_->expire_time > other.msg_->expire_time;
}

template <class T>
struct Greater {
    bool operator()(const T& a, const T& b) {
        return a < b;
    }
};

template <class T>
struct Timer: public std::priority_queue<T> {
public:
    std::deque<T>& GetQueue() { return this->c; }

    std::priority_queue<T, std::deque<T>, Greater<T>> timer_;
};

class WatcherSet {
public:
    WatcherSet() = default;
    ~WatcherSet() = default;
    void AddWatcher(std::string &, common::ProtoMessage*);
    WATCH_CODE DelWatcher(const int64_t &, const std::string &);
    uint32_t GetWatchers(std::vector<common::ProtoMessage*>& , const std::string &);

private:
    Key2Watchers_ key_index_;
    Watcher2Keys_ watcher_index_;
    std::mutex mutex_;

    Timer<Watcher> timer_;
    std::condition_variable timer_cond_;
    std::thread watchers_expire_thread_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
