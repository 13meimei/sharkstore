
#include "base/status.h"
#include "range/range.h"
#include "watcher_set.h"
#include "common/socket_session_impl.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

WatcherSet::WatcherSet() {
    watcher_timer_ = std::thread([this]() {
        while (watcher_timer_continue_flag_) {
            std::unique_lock<std::mutex> lock(watcher_mutex_);

            // watcher queue is empty, sleep 10ms
            if (watcher_queue_.empty()) {
                watcher_expire_cond_.wait_for(lock, std::chrono::milliseconds(10));
                continue;
            }

            // find the first wait watcher
            Watcher* w_ptr = nullptr;
            while (!watcher_queue_.empty()) {
                w_ptr = watcher_queue_.top();
                if (w_ptr->IsSentResponse()) {
                    // repsonse is sent, delete watcher in map and queue
                    // todo delete in map
                    watcher_queue_.pop();
                    delete w_ptr;
                    w_ptr = nullptr;
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
                //todo FLOG_DEBUG("watcher expire thread timeout: range_id[%" PRIu64"] session_id:[%" PRIu64 "] message_id:%" PRIu64, w.msg_->session_id, w.msg_->msg_id);

                // send timeout response
                auto resp = new watchpb::DsWatchResponse;
                resp->mutable_resp()->set_code(Status::kTimedOut);
                w_ptr->Send(resp);
                // todo delete in map
                watcher_queue_.pop();
                delete w_ptr;
            }
        }
    });
}

WatcherSet::~WatcherSet() {
    watcher_timer_continue_flag_ = false;
    watcher_timer_.join();
    // todo leave members' memory alone now, todo free
}

bool WatcherSet::AddWatcher(RangeId range_id, const Key& key, Watcher* w_ptr) {
    std::lock_guard<std::mutex> lock(watcher_mutex_);
    auto watcher_id = w_ptr->GetMessage()->session_id;

    // add to watcher map
    watcher_map_.emplace(std::make_pair(range_id, new RangeWatcherMap));
    auto range_watcher_map = watcher_map_.at(range_id);

    range_watcher_map->emplace(std::make_pair(key, new KeyWatcherMap));
    auto key_watcher_map = range_watcher_map->at(key);

    auto ok = key_watcher_map->insert(std::make_pair(watcher_id, w_ptr)).second;
    if (ok) {
        // add to queue
        watcher_queue_.push(w_ptr);

        // add to key map
        key_map_.emplace(std::make_pair(watcher_id, new RangeKeyMap));
        auto range_key_map = key_map_.at(watcher_id);

        range_key_map->emplace(std::make_pair(range_id, new WatcherKeyMap));
        auto watcher_key_map = range_key_map->at(range_id);

        watcher_key_map->insert(std::make_pair(key, nullptr));
    }
    return ok;
}

void WatcherSet::DelWatcher(RangeId range_id, const Key& key, WatcherId watch_id) {
    std::lock_guard<std::mutex> lock(watcher_mutex_);


}

void GetWatchersByKey(RangeId, std::vector<Watcher*>& , const Key&) {

}

} // namespace watch
}
}