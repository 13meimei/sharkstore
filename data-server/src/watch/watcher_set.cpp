
#include "base/status.h"
#include "range/range.h"
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
//                watcher_expire_cond_.wait_for(lock, std::chrono::milliseconds(10));
                watcher_expire_cond_.wait(lock);
            }

            // find the first wait watcher
            WatcherPtr w_ptr = nullptr;
            while (!watcher_queue_.empty()) {
                w_ptr = watcher_queue_.top();
                if (w_ptr->IsSentResponse()) {
                    // repsonse is sent, delete watcher in map and queue
                    // delete in map
                    Key encode_key;
                    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
                    if (w_ptr->GetType() == WATCH_KEY) {
                        DelKeyWatcher(encode_key, w_ptr->GetMessage()->session_id);
                    } else {
                        DelPrefixWatcher(encode_key, w_ptr->GetMessage()->session_id);
                    }

                    watcher_queue_.pop();
                    w_ptr = nullptr;

                    FLOG_INFO("watcher is sent response, timer queue pop : session_id:[%" PRIu64 "] key: [%s]",
                               w_ptr->GetMessage()->session_id, encode_key.c_str());
                } else {
                    break;
                }
            }
            if (w_ptr == nullptr) {
                continue; // no valid watcher wait in queue
            }

            auto mill_sec = std::chrono::milliseconds(w_ptr->GetMessage()->expire_time);
            std::chrono::system_clock::time_point expire(mill_sec);

            if (watcher_expire_cond_.wait_until(lock, expire) == std::cv_status::timeout) {
                // send timeout response
                auto resp = new watchpb::DsWatchResponse;
                resp->mutable_resp()->set_code(Status::kTimedOut);
                w_ptr->Send(resp);

                // delete in map
                Key encode_key;
                w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
                if (w_ptr->GetType() == WATCH_KEY) {
                    DelKeyWatcher(encode_key, w_ptr->GetMessage()->session_id);
                } else {
                    DelPrefixWatcher(encode_key, w_ptr->GetMessage()->session_id);
                }

                watcher_queue_.pop();

                FLOG_INFO("watcher expire timeout, timer queue pop: session_id:[%" PRIu64 "] key: [%s]",
                           w_ptr->GetMessage()->session_id, encode_key.c_str());
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
WatchCode WatcherSet::AddWatcher(const Key& key, WatcherPtr& w_ptr, WatcherMap& watcher_map_, KeyMap& key_map_) {
    std::unique_lock<std::mutex> lock_queue(watcher_queue_mutex_);
    std::lock_guard<std::mutex> lock_map(watcher_map_mutex_);

    auto watcher_id = w_ptr->GetMessage()->session_id;

    // add to watcher map
    watcher_map_.emplace(std::make_pair(key, new KeyWatcherMap));
    auto watcher_map = watcher_map_.at(key);

    auto ok = watcher_map->insert(std::make_pair(watcher_id, w_ptr)).second;
    WatchCode code;
    if (ok) {
        // add to queue
        watcher_expire_cond_.notify_one();
        watcher_queue_.push(w_ptr);

        // add to key map
        key_map_.emplace(std::make_pair(watcher_id, new WatcherKeyMap));
        auto watcher_key_map = key_map_.at(watcher_id);

        watcher_key_map->insert(std::make_pair(key, nullptr));

        code = WATCH_OK;

        FLOG_INFO("watcher add success: session_id:[%" PRIu64 "] key: [%s]",
                  w_ptr->GetMessage()->session_id, key.c_str());
    } else {
        code = WATCH_WATCHER_EXIST;

        FLOG_WARN("watcher add failed: session_id:[%" PRIu64 "] key: [%s]",
                  w_ptr->GetMessage()->session_id, key.c_str());
    }
    return code;
}

WatchCode WatcherSet::DelWatcher(const Key& key, WatcherId watcher_id, WatcherMap& watcher_map_, KeyMap& key_map_) {
    std::lock_guard<std::mutex> lock(watcher_map_mutex_);

    // XXX del from queue, pop in watcher expire thread

    // del from key map
    auto key_map_it = key_map_.find(watcher_id);
    if (key_map_it == key_map_.end()) {
        FLOG_WARN("watcher del failed, watcher id is not existed in key map: session_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, key.c_str());
        return WATCH_WATCHER_NOT_EXIST; // no watcher id in key map
    }
    auto& keys = key_map_it->second;
    auto key_it = keys->find(key);
    if (key_it == keys->end()) {
        FLOG_WARN("watcher del failed, key is not existed in key map: session_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, key.c_str());
        return WATCH_KEY_NOT_EXIST; // no key in key map
    }

    // del from watcher map
    auto watcher_map_it = watcher_map_.find(key);
    if (watcher_map_it == watcher_map_.end()) {
        FLOG_WARN("watcher del failed, key is not existed in watcher map: session_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, key.c_str());
        return WATCH_KEY_NOT_EXIST; // no key in watcher map
    }
    auto& watchers = watcher_map_it->second;
    auto watcher_it = watchers->find(watcher_id);
    if (watcher_it == watchers->end()) {
        FLOG_WARN("watcher del failed, watcher id is not existed in watcher map: session_id:[%" PRIu64 "] key: [%s]",
                  watcher_id, key.c_str());
        return WATCH_WATCHER_NOT_EXIST; // no watcher id in watcher map
    }

    // do del from key map
    keys->erase(key_it);
    // do del from watcher map
    watchers->erase(watcher_it);

    if (keys->empty()) {
       key_map_.erase(key_map_it);
    }
    if (watchers->empty()) {
        watcher_map_.erase(watcher_map_it);
    }

    FLOG_INFO("watcher del success: session_id:[%" PRIu64 "] key: [%s]",
              watcher_id, key.c_str());

    return WATCH_OK;
}

WatchCode WatcherSet::GetWatchers(std::vector<WatcherPtr>& vec, const Key& key, WatcherMap& key_map) {
    std::lock_guard<std::mutex> lock(watcher_map_mutex_);

    auto key_map_it = key_map.find(key);
    if (key_map_it == key_map.end()) {
        FLOG_WARN("watcher get failed, key is not existed in key map: key: [%s]", key.c_str());
        return WATCH_KEY_NOT_EXIST;
    }

    auto watchers = key_map_it->second;
    for (auto it = watchers->begin(); it != watchers->end(); ++it) {
        vec.push_back(it->second);
    }

    FLOG_WARN("watcher get success: key: [%s]", key.c_str());
    return WATCH_OK;
}

// key add/del watcher
WatchCode WatcherSet::AddKeyWatcher(const Key& key, WatcherPtr& w_ptr) {
    return AddWatcher(key, w_ptr, key_watcher_map_, key_map_);
}

WatchCode WatcherSet::DelKeyWatcher(const Key& key, WatcherId id) {
    return DelWatcher(key, id, key_watcher_map_, key_map_);
}

// key get watchers
WatchCode WatcherSet::GetKeyWatchers(std::vector<WatcherPtr>& vec, const Key& key) {
    return GetWatchers(vec, key, key_watcher_map_);
}

// prefix add/del watcher
WatchCode WatcherSet::AddPrefixWatcher(const Prefix& prefix, WatcherPtr& w_ptr) {
    return AddWatcher(prefix, w_ptr, prefix_watcher_map_, prefix_map_);
}

WatchCode WatcherSet::DelPrefixWatcher(const Prefix& prefix, WatcherId id) {
    return DelWatcher(prefix, id, prefix_watcher_map_, prefix_map_);
}

// prefix get watchers
WatchCode WatcherSet::GetPrefixWatchers(std::vector<WatcherPtr>& vec, const Prefix& prefix) {
    return GetWatchers(vec, prefix, prefix_watcher_map_);
}

} // namespace watch
}
}