_Pragma("once");
#include <string>

#include <proto/gen/watchpb.pb.h>
#include <common/socket_session.h>
#include <mutex>

namespace sharkstore {
namespace dataserver {
namespace range {

//<<<<<<< HEAD
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
//=======
    enum WATCH_CODE {
        WATCH_OK = 0,
        WATCH_KEY_NOT_EXIST = -1,
        WATCH_WATCHER_NOT_EXIST = -2
    };

    typedef std::unordered_map<int64_t, common::ProtoMessage*> WatcherSet_;
    typedef std::unordered_map<std::string, WatcherSet_> Key2Watchers_;
    typedef std::unordered_map<std::string, nullptr_t> KeySet_;
    typedef std::unordered_map<int64_t, KeySet_> Watcher2Keys_;

    class WatcherSet {
    public:
        WatcherSet() {};
        ~WatcherSet() {};
        void AddWatcher(std::string &, common::ProtoMessage*);
        WATCH_CODE DelWatcher(const int64_t &, const std::string &);
        uint32_t GetWatchers(std::vector<common::ProtoMessage*>& , const std::string &);

    private:
        Key2Watchers_ key_index_;
        Watcher2Keys_ watcher_index_;
        std::mutex mutex_;
    };


    void WatcherSet::AddWatcher(std::string &name, common::ProtoMessage* msg) {
        std::lock_guard<std::mutex> lock(mutex_);

        // build key name to watcher session id
/*//<<<<<<< HEAD
//        auto kit0 = key_index_.find(name);
//        if (kit0 == key_index_.end()) {
            auto kit_pair0 = key_index_.emplace(std::make_pair(name, std::make_pair(msg->session_id, msg)));
        auto kit0 = kit_pair0.first;
//        } else {
=======
 */
        auto kit0 = key_index_.find(name);
        if (kit0 == key_index_.end()) {
            auto tmpPair = key_index_.emplace(std::make_pair(name, std::make_pair(msg->session_id, msg)));
            if (tmpPair->second) 
                kit0 = tmpPair->first;
            else
                FLOG_DEBUG("AddWatcher fail key:%s session_id:%lld .", name.data(), msg->session_id);
        } else {
//>>>>>>> 82ef34844b3b8b52d54439ec9f1b04dbffb7360b
            auto kit1 = kit0->second.find(msg->session_id);
            if (kit1 == kit0->second.end()) {
                kit0->second.emplace(std::make_pair(msg->session_id, msg));
            }
//        }

        // build watcher id to key name
        auto wit0 = watcher_index_.find(msg->session_id);
        if (wit0 == watcher_index_.end()) {
            auto tmpPair = watcher_index_.emplace(std::make_pair(msg->session_id, std::make_pair(name, nullptr)));
            if (tmpPair->second)
                wit0 = tmpPair->first;
            else
                FLOG_DEBUG("AddWatcher fail session_id:%lld key:%s .", msg->session_id, name.data());;
        } else {
            auto wit1 = wit0->second.find(name);
            if (wit1 == wit0->second.end()) {
                wit0->second.emplace(std::make_pair(name, nullptr));
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
        /*for (auto key = keys.begin(); key != keys.end(); ++key) {
            auto kit = key_index_.find(key->first);
            //assert(kit != key_index_.end());
            auto watchers = kit->second;
            watchers.erase(id);
        }*/
        auto itKeys = keys.find(key);
        if (itKeys != keys.end()) {
            auto itKeyIdx = key_index_.find(key);
            if (itKeyIdx != key_index_.end()) {
                auto itSession = itKeyIdx->second.find(id);
                if (itSession != itKeyIdx->second.end()) {
                    itKeyIdx->second.erase(itSession)
                }

                key_index_.erase(itKeyIdx);
            }
            keys.erase(itKeys);
        } 

        if(keys.size() < 1) {
            watcher_index_.erase(wit);
        }

        return WATCH_OK;
    }

    uint32_t WatcherSet::GetWatchers(std::vector<common::ProtoMessage*>& vec, const std::string &name) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto kit = key_index_.find(name);
        if (kit == key_index_.end()) {
            return WATCH_KEY_NOT_EXIST;
        }
        
        auto watchers = kit->second;
        uint32_t msgSize = watchers.size();

        if (msgSize > 0) {
            vec.resize(msgSize);

            for (auto it = watchers.begin(); it != watchers.end(); ++it) {
                vec.emplace_back(it->second);
            }
        }
        
        return msgSize;
    }

} // namespace end
}
}
//>>>>>>> 01d07887970bbd66062fde0030ec36fd194ea720
