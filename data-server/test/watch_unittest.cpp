#include <gtest/gtest.h>

#include "range/watch.h"
#include "frame/sf_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
using namespace sharkstore::dataserver;

class TestWatcherSet: private range::WatcherSet {
public:
    bool CheckAddWatcher(std::string& watcher_key, int64_t watcher_id) {
        // in key index
        auto k_idx = key_index_.find(watcher_key);
        ASSERT_TRUE(k_idx != key_index_.end());

        auto watchers = k_idx->second;
        auto watcher_it = watchers.find(watcher_id);
        ASSERT_TRUE(watcher_it != watchers.end());

        // in watch index
        auto w_idx = watcher_idx_.find(watcher_id);
        ASSERT_TRUE(w_idx != watcher_idx_.end());

        auto keys = w_idx->second;
        auto key = keys.find(watcher_key);
        ASSERT_TRUE(key != keys.end());

        // in timer queue
        auto timer_queue = timer_.GetQueue();
        for (auto timer_it = timer_queue.begin(); timer_it != timer_queue.end(); ++timer_it) {
            if (timer_it->second->msg_->session_id == watcher_id && (*timer_it->second->key_) == watcher_key) {
                break;
            }
        }
        ASSERT_TRUE(timer_it != timer_.timer_.end());
    }

    bool CheckDelWatcher(std::string&, int64_t seesion_id) {
        // not in key index
        auto k_idx = key_index_.find(watcher_key);
        if (k_idx != key_index_.end()) {
            auto watchers = k_idx->second;
            auto watcher_it = watchers.find(watcher_id);
            ASSERT_TRUE(watcher_it == watchers.end());
        }

        // not in watch index
        auto w_idx = watcher_idx_.find(watcher_id);
        if (w_idx != watcher_idx_.end()) {
            auto keys = w_idx->second;
            auto key = keys.find(watcher_key);
            ASSERT_TRUE(key == keys.end());
        }

        // in timer queue
        auto timer_queue = timer_.GetQueue();
        for (auto timer_it = timer_queue.begin(); timer_it != timer_queue.end(); ++timer_it) {
            ASSERT_FALSE(timer_it->second->msg_->session_id == watcher_id && (*timer_it->second->key_) == watcher_key);
        }
        ASSERT_TRUE(timer_it == timer_.timer_.end());
    }
};

TEST(TestWatcherSet, Base) {
    TestWatcherSet ws;

    common::ProtoMessage msg = {
            session_id = 1,
            begin_time = getticks(), // ms
            expire_time = getticks()+100000, // 100 s
            msg_id = 100,
    };

    std::string key("testkey");
    // add watcher
    auto add_ret = ws.AddWatcher(key, &msg);
    ASSERT_EQ(0, add_ret);

    ws.CheckAddWatcher(key, 1);

    // del watcher
    auto del_ret = ws.DelWatcher(key, 1);
    ASSERT_EQ(0, del_ret);

    ws.CheckDelWatcher(key, 1);

}

TEST(TestTimer, Base) {
    using namespace range;
    Timer<Watcher> timer;

    common::ProtoMessage m1 = {
            .expire_time = 10,
    };
    auto k1 = new std::string("key1");

    common::ProtoMessage m2 = {
            .expire_time = 5,
    };
    auto k2 = new std::string("key2");
    timer.push(Watcher{&m1, k1});
    timer.push(Watcher{&m2, k2});

    auto queue = timer.GetQueue();
    ASSERT_TRUE(queue.size() == 2);

    auto i = 0;
    for (auto it = queue.begin(); it != queue.end(); ++it) {
        if (i == 0) { ASSERT_TRUE(it->key_ == k2 && it->msg_->expire_time == 5); }
        if (i == 1) { ASSERT_TRUE(it->key_ == k1 && it->msg_->expire_time == 10); }
        ++i;
    }
}

TEST(TestEncodeDecodeKey, Base) {
//    void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);
//    bool DecodeWatchKey(std::vector<std::string*>& keys, std::string* buf);
    using namespace range;
    std::string buf;
    uint64_t table_id = 1;
    std::vector<std::string*> keys;
    keys.push_back(new std::string("key1"));
    EncodeWatchKey(&buf, 1, keys);

    std::vector<std::string*> de_keys;
    DecodeWatchKey(de_keys, &buf);
    ASSERT_TRUE(de_keys.size() == 1);
    ASSERT_TRUE(*(de_keys[0])->c_str() == "key1");
}

TEST(TestEncodeDecodeValue, Base) {
//    void EncodeWatchValue(std::string *buf,
//                          int64_t &version,
//                          const std::string *value,
//                          const std::string *extend);
//    bool DecodeWatchValue(int64_t *version, std::string *value, std::string *extend,
//                          std::string &buf);
    std::string buf;

    int64_t ver = 123;
    auto v = std::string("value123");
    auto ext = std::string("extend123");
    EncodeWatchValue(buf, ver, &v, &ext);

    int64_t de_ver;
    std::string de_v;
    std::string de_ext;
    DecodeWatchValue(&de_ver, &de_v, &de_ext, buf);
    ASSERT_EQ(de_ver, 123);
    ASSERT_EQ(de_v, v);
    ASSERT_EQ(de_ext, ext);
}

} /* namespace  */
