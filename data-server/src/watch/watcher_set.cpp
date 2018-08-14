
#include "base/status.h"
#include "range/range.h"
#include "range/watch.h"

#include "watcher_set.h"
#include "common/socket_session_impl.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

WatcherSet::WatcherSet() {
    watcher_timer_ = std::thread([this]() {
        while (watcher_timer_continue_flag_) {
            std::unique_lock<std::mutex> lock(watcher_queue_mutex_);

            // watcher queue is empty, sleep 10ms
            if (watcher_queue_.empty()) {
                watcher_expire_cond_.wait_for(lock, std::chrono::milliseconds(1000));
                //watcher_expire_cond_.wait(lock);
            }

            // find the first wait watcher
            WatcherPtr w_ptr = nullptr;
            while (!watcher_queue_.empty()) {
                w_ptr = watcher_queue_.top();
                if (w_ptr->IsSentResponse()) {
                    // repsonse is sent, delete watcher in map and queue
                    // delete in map
                    WatcherKey encode_key;
                    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys(false));
                    if (w_ptr->GetType() == WATCH_KEY) {
                        DelKeyWatcher(encode_key, w_ptr->GetWatcherId());
                    } else {
                        DelPrefixWatcher(encode_key, w_ptr->GetWatcherId());
                    }

                    watcher_queue_.pop();

                    FLOG_INFO("watcher is sent response, timer queue pop : watch_id:[%" PRIu64 "] key: [%s]",
                               w_ptr->GetWatcherId(), EncodeToHexString(encode_key).c_str());
                    w_ptr = nullptr;
                } else {
                    break;
                }
            }
            if (w_ptr == nullptr) {
                continue; // no valid watcher wait in queue
            }

            auto mill_sec = std::chrono::milliseconds(w_ptr->GetExpireTime() / 1000);
            std::chrono::system_clock::time_point expire(mill_sec);

            int64_t  waitBeginTime{w_ptr->GetMessage()->begin_time / 1000};

            if (watcher_expire_cond_.wait_until(lock, expire) == std::cv_status::timeout) {
                auto excBegin = getticks();
                // send timeout response
                auto resp = new watchpb::DsWatchResponse;
                resp->mutable_resp()->set_code(Status::kTimedOut);

//                auto err = new errorpb::Error();
//                err->set_message("watch request timeout");
//                resp->mutable_header()->set_allocated_error(err);

                w_ptr->Send(resp);

                // delete in map
                WatcherKey encode_key;
                w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
                if (w_ptr->GetType() == WATCH_KEY) {
                    DelKeyWatcher(encode_key, w_ptr->GetWatcherId());
                } else {
                    DelPrefixWatcher(encode_key, w_ptr->GetWatcherId());
                }

                watcher_queue_.pop();

                auto excEnd = getticks();
                //auto take_time = excEnd - waitBeginTime;

                FLOG_DEBUG("wait_until....session_id: %" PRId64 ",task msgid: %" PRId64 " watcher_id:%" PRId64
                                   " execute take time: %" PRId64 " ms,wait time:%" PRId64 ,
                           w_ptr->GetMessage()->session_id, w_ptr->GetMessage()->msg_id, w_ptr->GetWatcherId(), excEnd-excBegin,excBegin-waitBeginTime);

                FLOG_INFO("watcher expire timeout, timer queue pop: session_id: %" PRId64 " watch_id:[%" PRIu64 "] key: [%s]",
                          w_ptr->GetMessage()->session_id, w_ptr->GetWatcherId(), EncodeToHexString(encode_key).c_str());
            }
        }
    });
}

WatcherSet::~WatcherSet() {
    watcher_timer_continue_flag_ = false;
    {
        std::unique_lock<std::mutex> lock(watcher_queue_mutex_);
        watcher_expire_cond_.notify_one();
    }
    watcher_timer_.join();
    // todo leave members' memory alone now, todo free
}


// private add/del watcher
WatchCode WatcherSet::AddWatcher(const WatcherKey& key, WatcherPtr& w_ptr, WatcherMap& key_watchers, KeyMap& key_map, storage::Store *store_, bool prefixFlag ) {
    std::unique_lock<std::mutex> lock_queue(watcher_queue_mutex_);
    std::lock_guard<std::mutex> lock_map(watcher_map_mutex_);

    WatchCode code;
    auto watcher_id = w_ptr->GetWatcherId();
    auto currKeyVer = w_ptr->getKeyVersion();

    // add to watcher map
    auto watcher_map_it = key_watchers.find(key);
    if (watcher_map_it == key_watchers.end()) {

        std::string val;
        std::string userVal("");
        std::string ext("");
        int64_t version(0);
        if(store_!= nullptr){

            //single key
            Status ret = store_->Get(key, &val);
            if(ret.ok()){
                if (!watch::Watcher::DecodeValue(&version, &userVal, &ext, val)) {
                    FLOG_ERROR("AddWatcher Decode error, key: %s", EncodeToHexString(key).c_str());
                    version = 0;
                    return WATCH_WATCHER_NOT_NEED;
                }
            }else if(ret.code() != Status::kNotFound) {
                FLOG_ERROR("AddWatcher Decode error, key: %s", EncodeToHexString(key).c_str());
                version = 0;
                return WATCH_WATCHER_NOT_NEED;
            }

            if(prefixFlag) {
                FLOG_DEBUG("prefix mode: loadFromDb...");
                std::string endKey(key);

                if( 0 != range::WatchEncodeAndDecode::NextComparableBytes(key.data(), key.length(), endKey)) {
                    //to do set error message
                    FLOG_ERROR("AddWatcher fail, NextComparableBytes execute error.");
                    return WATCH_WATCHER_NOT_NEED;
                }

                auto ds_resp = new watchpb::DsWatchResponse;
                auto count = loadFromDb(store_, watchpb::PUT, key, endKey, version, w_ptr->GetTableId(), ds_resp);
                if(count > 0) {
                    w_ptr->Send(ds_resp);
                    return WATCH_OK;
                }
            }

        }

        FLOG_DEBUG("AddWatcher db version[%" PRId64 "], key: %s", version, EncodeToHexString(key).c_str());
        auto v = new WatcherValue;
        v->key_version_ = version;
        watcher_map_it = key_watchers.insert(std::make_pair(key, v)).first;
    }
    auto& watcher_map = watcher_map_it->second->mapKeyWatcher;

     if( currKeyVer < watcher_map_it->second->key_version_ ) {

//         if(currKeyVer > watcher_map_it->second->key_version_) {
//             FLOG_ERROR("watcher add skip: watcher_id:[%" PRIu64 "] key: [%s] current version[%" PRIu64 "] watcher version[%" PRIu64 "]",
//                       w_ptr->GetWatcherId(), EncodeToHexString(key).c_str(), currKeyVer, watcher_map_it->second->key_version_);
//         } else {
             FLOG_INFO("watcher add skip: watcher_id:[%"
                               PRIu64
                               "] key: [%s] current version[%"
                               PRIu64
                               "] watcher version[%"
                               PRIu64
                               "]",
                       w_ptr->GetWatcherId(), EncodeToHexString(key).c_str(), currKeyVer,
                       watcher_map_it->second->key_version_);

//         }
         return WATCH_WATCHER_NOT_NEED;
     }

    auto ret = watcher_map.emplace(std::make_pair(watcher_id, w_ptr)).second;
    if (ret) {
        //add to key_map_
        //key_map.emplace(std::make_pair(watcher_id, key));

        // add to queue
        watcher_queue_.push(w_ptr);
        watcher_expire_cond_.notify_one();

        code = WATCH_OK;

        FLOG_INFO("watcher add success: count:%" PRIu64 " watcher_id:[%" PRIu64 "] key: [%s]",
                  watcher_map_it->second->mapKeyWatcher.size(), w_ptr->GetWatcherId(), EncodeToHexString(key).c_str());
    } else {
        code = WATCH_WATCHER_EXIST;

        FLOG_ERROR("watcher add failed: watcher_id:[%" PRIu64 "] key: [%s]",
                  w_ptr->GetWatcherId(), EncodeToHexString(key).c_str());
    }
    return code;
}

WatchCode WatcherSet::DelWatcher(const WatcherKey& key, WatcherId watcher_id, WatcherMap& watcher_map_, KeyMap& key_map_) {
    std::lock_guard<std::mutex> lock(watcher_map_mutex_);

    // XXX del from queue, pop in watcher expire thread

    // del from key map
    /*auto key_map_it = key_map_.find(watcher_id);
    if (key_map_it == key_map_.end()) {
        FLOG_WARN("watcher del failed, watcher id is not existed in key map: watch_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, EncodeToHexString(key).c_str());
        //return WATCH_WATCHER_NOT_EXIST; // no watcher id in key map
    } else {
        auto &keys = key_map_it->second;
        auto key_it = keys->find(key);
        if (key_it == keys->end()) {
            FLOG_WARN("watcher del failed, key is not existed in key map: watch_id:[%"
                              PRIu64
                              "] key: [%s]",
                      watcher_id, EncodeToHexString(key).c_str());
            return WATCH_KEY_NOT_EXIST; // no key in key map
        }

        // do del from key map
        keys->erase(key_it);

        //erase key:watchid
        if (keys->empty()) {
            key_map_.erase(key_map_it);
        }

        FLOG_WARN("watcher del success within key map: watch_id:[%"
                          PRIu64
                          "] key: [%s]",
                  watcher_id, EncodeToHexString(key).c_str());
    }
    */

    // del from watcher map
    auto watcher_map_it = watcher_map_.find(key);
    if (watcher_map_it == watcher_map_.end()) {
        FLOG_WARN("watcher del failed, key is not existed in watcher map: watch_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, EncodeToHexString(key).c_str());

    } else {
        //mapKeyWatcher maybe already swaped when getWatcher method called, except timeout occasion
        auto &watchers = watcher_map_it->second->mapKeyWatcher;
        if(watchers.size() > 0) {
            auto watcher_it = watchers.find(watcher_id);
            if (watcher_it == watchers.end()) {
            FLOG_WARN("watcher del failed, watcher id is not existed in keyWatcher map. map_size:%" PRIu64 " watch_id:[%"
                              PRIu64
                              "] key: [%s]",
                      watchers.size(), watcher_id, EncodeToHexString(key).c_str());
                ;
            } else {
                // do del from watcher map
                watchers.erase(watcher_it);

                FLOG_WARN("watcher del success within watcher map: watch_id:[%"
                                  PRIu64
                                  "] key: [%s]",
                          watcher_id, EncodeToHexString(key).c_str());
            }
        }
    }

    /*
    if (watchers->mapKeyWatcher.empty()) {
        watcher_map_.erase(watcher_map_it);
    }*/

    FLOG_INFO("watcher del end: watch_id:[%" PRIu64 "] key: [%s]",
              watcher_id, EncodeToHexString(key).c_str());

    return WATCH_OK;
}

WatchCode WatcherSet::GetWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& vec, const WatcherKey& key, WatcherMap& watcherMap, WatcherValue *watcherValue, bool prefixFlag) {
    std::lock_guard<std::mutex> lock(watcher_map_mutex_);

    auto itWatcherVal = watcherMap.find(key);
    if (itWatcherVal == watcherMap.end()) {
        FLOG_INFO("GetWatcher end,key[%s] has no watcher.", EncodeToHexString(key).c_str());
        return WATCH_KEY_NOT_EXIST;
    }

    //watcherId:watchPtr
    auto watchers = itWatcherVal->second;
    if(watchers->key_version_ < watcherValue->key_version_ && !prefixFlag) {
        watchers->key_version_ = watcherValue->key_version_;
    }

    //to do clear version if delete event
    if(evtType == watchpb::DELETE) {
        watchers->key_version_ = 0;
    }

    if(watchers->mapKeyWatcher.size() > 0) {
        watchers->mapKeyWatcher.swap(watcherValue->mapKeyWatcher);

        FLOG_INFO("watcher get success,count:%" PRIu64 " key: [%s] watch_id[%" PRId64 "]",
                  watcherValue->mapKeyWatcher.size(), EncodeToHexString(key).c_str(), watcherValue->mapKeyWatcher.begin()->first );
        return WATCH_OK;
    }

    FLOG_INFO("GetWatcher end, key [%s] has no watcher...", EncodeToHexString(key).c_str());
    return WATCH_WATCHER_NOT_EXIST;

}

// key add/del watcher
WatchCode WatcherSet::AddKeyWatcher(const WatcherKey& key, WatcherPtr& w_ptr, storage::Store *store_) {
    return AddWatcher(key, w_ptr, key_watcher_map_, key_map_, store_);
}

WatchCode WatcherSet::DelKeyWatcher(const WatcherKey& key, WatcherId id) {
    return DelWatcher(key, id, key_watcher_map_, key_map_);
}

// key get watchers
WatchCode WatcherSet::GetKeyWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& vec, const WatcherKey& key, const int64_t &version) {
    auto watcherVal = new WatcherValue;
    //auto mapKeyWatcher = new KeyWatcherMap;
    watcherVal->key_version_ = version;

    auto retCode = GetWatchers(evtType, vec, key, key_watcher_map_, watcherVal);
    if( WATCH_OK == retCode) {

        for(auto it:watcherVal->mapKeyWatcher) {
            vec.push_back(it.second);
        }

        delete (watcherVal);
        watcherVal = nullptr;
    }

    if(watcherVal != nullptr)
        delete(watcherVal);

    return retCode;
}

// prefix add/del watcher
WatchCode WatcherSet::AddPrefixWatcher(const PrefixKey& prefix, WatcherPtr& w_ptr, storage::Store *store_) {
    return AddWatcher(prefix, w_ptr, prefix_watcher_map_, prefix_map_, store_, true);
}

WatchCode WatcherSet::DelPrefixWatcher(const PrefixKey& prefix, WatcherId id) {
    return DelWatcher(prefix, id, prefix_watcher_map_, prefix_map_);
}

// prefix get watchers
WatchCode WatcherSet::GetPrefixWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& vec, const PrefixKey& prefix, const int64_t &version) {
    auto watcherVal = new WatcherValue;
    watcherVal->key_version_ = version;

    auto retCode = GetWatchers(evtType, vec, prefix, prefix_watcher_map_, watcherVal, true);
    if( WATCH_OK == retCode) {

        for(auto it:(watcherVal->mapKeyWatcher)) {
            vec.push_back(it.second);
        }

        delete (watcherVal);
        watcherVal = nullptr;
    }

    if(watcherVal != nullptr)
        delete(watcherVal);

    return retCode;
}

int32_t WatcherSet::loadFromDb(storage::Store *store, const watchpb::EventType &evtType, const std::string &fromKey,
                   const std::string &endKey, const int64_t &startVersion, const uint64_t &tableId,
                   watchpb::DsWatchResponse *dsResp) {

    //need to encode and decode
    std::shared_ptr<storage::Iterator> iterator(store->NewIterator(fromKey, endKey));
    int32_t count{0};
    int64_t minVersion{0};
    int64_t maxVersion{0};
    auto err = std::make_shared<errorpb::Error>();

    auto resp = dsResp->mutable_resp();
    resp->set_code(Status::kOk);
    resp->set_scope(watchpb::RESPONSE_ALL);

    for (int i = 0; iterator->Valid() ; ++i) {

        auto evt = resp->add_events();
        evt->set_type(evtType);

        auto tmpDbKey = iterator.get()->key();
        auto tmpDbValue = iterator.get()->value();

        watchpb::WatchKeyValue kv;
        if(Status::kOk != range::WatchEncodeAndDecode::DecodeKv(funcpb::kFuncPureGet, tableId, &kv, tmpDbKey, tmpDbValue, err.get())) {
            //break;
            continue;
        }

        if (minVersion > kv.version()) {
            minVersion = kv.version();
        }
        if(maxVersion < kv.version()) {
            maxVersion = kv.version();
        }

        if( kv.version() > startVersion) {

            for (int16_t i = 0; i < kv.key().size(); i++) {
                evt->mutable_kv()->add_key(kv.key(i));
            }

            evt->mutable_kv()->set_value(kv.value());
            evt->mutable_kv()->set_version(kv.version());
            evt->set_type(evtType);

            count++;
        }

        iterator->Next();
    }

    return count;
}

} // namespace watch
}
}
