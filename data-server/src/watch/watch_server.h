#ifndef _WATCH_SERVER_
#define _WATCH_SERVER_

#include <vector>
#include <mutex>

#include "watcher_set.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

#define WATCHER_SET_COUNT_MIN   1
#define WATCHER_SET_COUNT_MAX   64

class WatchServer {
public:
    WatchServer() = default;
    WatchServer(uint64_t watcher_set_count);
    WatchServer(const WatchServer&) = delete;
    WatchServer& operator=(const WatchServer&) = delete;
    ~WatchServer();

    WatchCode AddKeyWatcher(WatcherPtr&);
    WatchCode AddPrefixWatcher(WatcherPtr&);

    WatchCode DelKeyWatcher(WatcherPtr&);
    WatchCode DelPrefixWatcher(WatcherPtr&);

    WatchCode GetKeyWatchers(std::vector<WatcherPtr>&, const WatcherKey&);
    WatchCode GetPrefixWatchers(std::vector<WatcherPtr>&, const Prefix &);

private:
    uint64_t                    watcher_set_count_ = WATCHER_SET_COUNT_MIN;
    std::vector<WatcherSet*>    watcher_set_list;

public:
    WatcherSet* GetWatcherSet_(const WatcherKey&);
};


} // namespace watch
}
}
#endif
