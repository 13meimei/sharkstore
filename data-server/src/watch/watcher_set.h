#ifndef _WATCHER_SET_H_
#define _WATCHER_SET_H_

#include <unordered_map>
#include <vector>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "watch.h"
#include "watcher.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

// todo watch to shark ptr
typedef std::unordered_map<WatcherId, Watcher*> KeyWatcherMap;
typedef std::unordered_map<Key, KeyWatcherMap*> RangeWatcherMap;
typedef std::unordered_map<RangeId, RangeWatcherMap*> WatcherMap;
typedef std::priority_queue<Watcher*, std::vector<Watcher*>, Greater<Watcher*>> WatcherQueue;

typedef std::unordered_map<Key, nullptr_t> WatcherKeyMap;
typedef std::unordered_map<RangeId, WatcherKeyMap*> RangeKeyMap;
typedef std::unordered_map<WatcherId, RangeKeyMap*> KeyMap;

class WatcherSet {
public:
    WatcherSet();
    ~WatcherSet();

    bool AddWatcher(RangeId, const Key&, Watcher*);
    void DelWatcher(RangeId, const Key&, WatcherId);
    void GetWatchersByKey(RangeId, std::vector<Watcher*>& , const Key&);

private:
    WatcherMap              watcher_map_;
    KeyMap                  key_map_;
    WatcherQueue            watcher_queue_;
    std::mutex              watcher_mutex_;

    std::thread                     watcher_timer_;
    volatile bool                   watcher_timer_continue_flag_ = true;
    std::condition_variable         watcher_expire_cond_;
};


} // namespace watch
}
}

#endif
