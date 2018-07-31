
#include "watch_server.h"
#include "frame/sf_logger.h"
#include "common/ds_encoding.h"

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
    FLOG_DEBUG("watch server add key watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_KEY);
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    w_ptr->SetWatcherId(ws->GenWatcherId());
    return ws->AddKeyWatcher(encode_key, w_ptr);
}

WatchCode WatchServer::AddPrefixWatcher(WatcherPtr& w_ptr) {
    FLOG_DEBUG("watch server add prefix watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_PREFIX);
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    w_ptr->SetWatcherId(ws->GenWatcherId());
    return ws->AddPrefixWatcher(encode_key, w_ptr);
}

WatchCode WatchServer::DelKeyWatcher(WatcherPtr& w_ptr) {
    FLOG_DEBUG("watch server del key watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_Key);
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->DelKeyWatcher(encode_key, w_ptr->GetWatcherId());
}

WatchCode WatchServer::DelPrefixWatcher(WatcherPtr& w_ptr) {
    FLOG_DEBUG("watch server del prefix watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_PREFIX);
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    return ws->DelPrefixWatcher(encode_key, w_ptr->GetWatcherId());
}

WatchCode WatchServer::GetKeyWatchers(std::vector<WatcherPtr>& w_ptr_vec, const Key& key) {
    FLOG_DEBUG("watch server get key watchers: key [%s]", EncodeToHexString(key).c_str());
    assert(w_ptr_vec.size() == 0);
    auto ws = GetWatcherSet_(key);
    return ws->GetKeyWatchers(w_ptr_vec, key);
}

WatchCode WatchServer::GetPrefixWatchers(std::vector<WatcherPtr>& w_ptr_vec, const Prefix& prefix) {
    FLOG_DEBUG("watch server get prefix watchers: key [%s]", EncodeToHexString(prefix).c_str());
    assert(w_ptr_vec.size() == 0);
    auto ws = GetWatcherSet_(prefix);
    return ws->GetPrefixWatchers(w_ptr_vec, prefix);
}



} // namepsace watch
}
}
