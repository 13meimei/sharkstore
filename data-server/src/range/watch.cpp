//#include "watch.h"
#include <common/socket_session.h>
#include "watch.h"
#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {
void Range::AddKeyWatcher(std::string &name, common::ProtoMessage *msg) {
    key_watchers_.AddWatcher(name, msg);
}

WATCH_CODE Range::DelKeyWatcher(const int64_t &id, const std::string &key) {
    return key_watchers_.DelWatcher(id, key);
}

uint32_t Range::GetKeyWatchers(std::vector<common::ProtoMessage *> &vec, std::string name) {
    return key_watchers_.GetWatchers(vec, name);
}

// encode keys into buffer
void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys) {
    assert(buf != nullptr && buf->length() != 0);
    assert(keys.size() != 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    for (auto key : keys) {
        EncodeBytesAscending(buf, key->c_str(), key->length());
    }
}

// decode buffer to keys
bool DecodeWatchKey(std::vector<std::string *> &keys, std::string *buf) {
    assert(keys.size() == 0 && buf->length() > 9);

    size_t offset;
    for (offset = 9; offset != buf->length() - 1;) {
        std::string* b;

        b = new std::string();
        if (!DecodeBytesAscending(*buf, offset, b)) {
            return false;
        }
        keys.push_back(b);
    }
    return true;
}

void EncodeWatchValue(std::string *buf,
                      int64_t &version,
                      const std::string *value,
                      const std::string *extend) {
    assert(buf != nullptr);
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value->c_str(), value->length());
    EncodeBytesValue(buf, 4, extend->c_str(), extend->length());
}

bool DecodeWatchValue(int64_t *version, std::string *value, std::string *extend,
                      std::string &buf) {
    assert(version != nullptr && value != nullptr && extend != nullptr &&
           buf.length() != 0);

    size_t offset = 0;
    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, value)) return false;
    if (!DecodeBytesValue(buf, offset, extend)) return false;
    return true;
}

WatcherSet::WatcherSet() {
    watchers_expire_thread_ = std::thread([this]() {
        while (g_continue_flag) {
            Watcher* w;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (timer_.empty()) {
                    timer_cond_.wait_for(lock, std::chrono::milliseconds(10));
                    continue; // sleep 10ms
                }

                w = timer_.top();
                if (timer_cond_.wait_until(lock, std::chrono::milliseconds(w->msg_->expire_time)) ==
                    std::cv_status::timeout) {

                    // todo send timeout response

                    // delete watcher
                    {
                        auto id = w->msg_->session_id;
                        auto key = w->key_;
                        auto wit = watcher_index_.find(id);
                        if (wit == watcher_index_.end()) {
                            continue;
                        }

                        auto keys = wit->second; // key map
                        auto itKeys = keys.find(*key);

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

                        if (keys.size() < 1) {
                            watcher_index_.erase(wit);
                        }
                    } // del watcher

                    timer_.pop();
                }
            }
        }
    }
}

WatcherSet::~WatcherSet() {
    watchers_expire_thread_.join();
}

void WatcherSet::AddWatcher(std::string &name, common::ProtoMessage *msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();
    timer_.push(Watcher(msg, &name));

    // build key name to watcher session id
    auto kit0 = key_index_.find(name);
    if (kit0 == key_index_.end()) {
        auto tmpPair = key_index_.emplace(std::make_pair(name, std::make_pair(msg->session_id, msg)));
        if (tmpPair->second) kit0 = tmpPair->first;
        else FLOG_DEBUG("AddWatcher abnormal key:%s session_id:%lld .", name.data(), msg->session_id);
    }

    if (kit0 != key_index_.end()) {
        auto kit1 = kit0->second.find(msg->session_id);
        if (kit1 == kit0->second.end()) {
            kit0->second.emplace(std::make_pair(msg->session_id, msg));
        }
        // build watcher id to key name
        auto wit0 = watcher_index_.find(msg->session_id);
        if (wit0 == watcher_index_.end()) {
            auto tmpPair = watcher_index_.emplace(std::make_pair(msg->session_id, std::make_pair(name, nullptr)));
            if (tmpPair->second) wit0 = tmpPair->first;
            else FLOG_DEBUG("AddWatcher abnormal session_id:%lld key:%s .", msg->session_id, name.data());
        }

        if (wit0 != watcher_index_.end()) {
            auto wit1 = wit0->second.find(name);
            if (wit1 == wit0->second.end()) {
                wit0->second.emplace(std::make_pair(name, nullptr));
            }
        }
    }

    return;
}

WATCH_CODE WatcherSet::DelWatcher(const int64_t &id, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();
//   todo timer_.find and del
    for (auto it: timer_.GetQueue()) {
        if (it->msg_->session_id == id && (*it.key_) == key) {
            timer_.GetQueue().erase(it);
        }
    }

    //<session:keys>
    auto wit = watcher_index_.find(id);
    if (wit == watcher_index_.end()) {
        return WATCH_WATCHER_NOT_EXIST;
    }

    auto keys = wit->second; // key map
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

    if (keys.size() < 1) {
        watcher_index_.erase(wit);
    }

    return WATCH_OK;
}

uint32_t WatcherSet::GetWatchers(std::vector<common::ProtoMessage *> &vec, const std::string &name) {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_cond_.notify_one();

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



