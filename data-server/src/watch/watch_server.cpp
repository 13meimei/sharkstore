
#include "watch_server.h"

namespace sharkstore {
namespace dataserver {
namespace watch {



WatchServer::WatchServer(uint64_t watcher_set_count): watcher_set_count_(watcher_set_count) {
    watcher_set_count_ = watcher_set_count_ > WATCHER_SET_COUNT_MIN ? watcher_set_count_ : WATCHER_SET_COUNT_MIN;
    watcher_set_count_ = watcher_set_count_ < WATCHER_SET_COUNT_MAX ? watcher_set_count_ : WATCHER_SET_COUNT_MAX;

    for (uint64_t i = 0; i < watcher_set_count_; ++i) {
        watcher_set_list.push_back(new WatcherSet());
    }
}

WatchServer::~WatchServer() {
    for (auto watcher_set: watcher_set_list) {
       delete(watcher_set);
    }
}

WatcherSet* WatchServer::GetWatcherSet_(const Key& key) {
    std::size_t hash = std::hash<std::string>{}(key);
    return watcher_set_list[hash % watcher_set_count_];
}

WatchCode WatchServer::AddKeyWatcher(WatcherPtr& w_ptr) {
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->AddKeyWatcher(encode_key, w_ptr);
}

WatchCode WatchServer::AddPrefixWatcher(WatcherPtr& w_ptr) {
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->AddPrefixWatcher(encode_key, w_ptr);
}

WatchCode WatchServer::DelKeyWatcher(WatcherPtr& w_ptr) {
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->DelKeyWatcher(encode_key, w_ptr->GetMessage()->session_id);
}

WatchCode WatchServer::DelPrefixWatcher(WatcherPtr& w_ptr) {
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->DelPrefixWatcher(encode_key, w_ptr->GetMessage()->session_id);
}

WatchCode WatchServer::GetKeyWatchers(std::vector<WatcherPtr>& w_ptr_vec, const Key& key) {
    assert(w_ptr_vec.size() == 0);
    auto ws = GetWatcherSet_(key);
    return ws->GetKeyWatchers(w_ptr_vec, key);
}

WatchCode WatchServer::GetPrefixWatchers(std::vector<WatcherPtr>& w_ptr_vec, const Prefix& prefix) {
    assert(w_ptr_vec.size() == 0);
    auto ws = GetWatcherSet_(prefix);
    return ws->GetPrefixWatchers(w_ptr_vec, prefix);
}



} // namepsace watch
}
}
