
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

WatcherSet* WatchServer::GetWatcherSet_(const WatcherKey& key) {
    std::size_t hash = std::hash<std::string>{}(key);
    return watcher_set_list[hash % watcher_set_count_];
}

WatchCode WatchServer::AddKeyWatcher(WatcherPtr& w_ptr, storage::Store *store_) {
    int64_t msgSessionId(w_ptr->GetWatcherId());
    std::string encode_key;
    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto wset = GetWatcherSet_(encode_key);
    w_ptr->SetWatcherId(wset->GenWatcherId());

    FLOG_DEBUG("watch server ready to add key watcher: session id [%" PRIu64 "] watch_id[%" PRIu64 "] key:%s", msgSessionId, w_ptr->GetWatcherId(), EncodeToHexString(encode_key).c_str());
    assert(w_ptr->GetType() == WATCH_KEY);

    return wset->AddKeyWatcher(encode_key, w_ptr, store_);
}

WatchCode WatchServer::AddPrefixWatcher(WatcherPtr& w_ptr, storage::Store *store_) {
    FLOG_DEBUG("watch server add prefix watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_PREFIX);
    std::string encode_key;

    w_ptr->EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());

    auto ws = GetWatcherSet_(encode_key);
    w_ptr->SetWatcherId(ws->GenWatcherId());
    return ws->AddPrefixWatcher(encode_key, w_ptr, store_);
}

WatchCode WatchServer::DelKeyWatcher(WatcherPtr& w_ptr) {
    FLOG_DEBUG("watch server del key watcher: session id [%" PRIu64 "]", w_ptr->GetWatcherId());
    assert(w_ptr->GetType() == WATCH_KEY);

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

WatchCode WatchServer::GetKeyWatchers(std::vector<WatcherPtr>& w_ptr_vec, const WatcherKey& key, const int64_t &version) {
    FLOG_DEBUG("watch server ready to get key watchers: key [%s]", EncodeToHexString(key).c_str());
    assert(w_ptr_vec.size() == 0);
    auto ws = GetWatcherSet_(key);
    return ws->GetKeyWatchers(w_ptr_vec, key, version);
}

WatchCode WatchServer::GetPrefixWatchers(std::vector<WatcherPtr>& w_ptr_vec, const PrefixKey& prefix, const int64_t &version) {
    FLOG_DEBUG("watch server get prefix watchers: key [%s]", prefix.c_str());
    assert(w_ptr_vec.size() == 0);
    auto wset = GetWatcherSet_(prefix);
    return wset->GetPrefixWatchers(w_ptr_vec, prefix, version);
}



} // namepsace watch
}
}
