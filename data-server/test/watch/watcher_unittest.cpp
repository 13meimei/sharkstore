#include <gtest/gtest.h>

#define private public
#include "watch/watcher.h"
#include "watch/watcher_set.h"
#include "watch/watch_server.h"
#include "frame/sf_util.h"
#include "fastcommon/logger.h"

int main(int argc, char* argv[]) {
    log_init2();

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::watch;

const uint64_t g_table_id = 123;

class TestWatcher: public Watcher {
public:
    TestWatcher() = delete;
    TestWatcher(const std::vector<Key*>& keys, common::ProtoMessage* message): Watcher(g_table_id, keys, message) {
    }
    ~TestWatcher() = default;

    void Send(google::protobuf::Message* resp) override;
};
typedef std::shared_ptr<TestWatcher> TestWatcherPtr;

void TestWatcher::Send(google::protobuf::Message* resp) {
    std::lock_guard<std::mutex> lock(send_lock_);
    if (sent_response_flag) {
        return;
    }
    sent_response_flag = true;
}

class TestWatcherSet: public WatcherSet {
public:
    void CheckAddKeyWatcher(TestWatcherPtr& w_ptr) {
        Key encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys());
        auto watcher_id = w_ptr->GetWatcherId();

        // check key map
        auto watcher_id_it = key_map_.find(watcher_id);
        ASSERT_TRUE(watcher_id_it != key_map_.end());

        auto watcher_key_map = watcher_id_it->second;
        auto key_it = watcher_key_map->find(encode_key);
        ASSERT_TRUE(key_it != watcher_key_map->end());
        std::cout << "check add key map ok: session id: [" <<
                  watcher_id << "] key: [" << encode_key << "]" << std::endl;

        // check watcher map
        auto key_watcher_it = key_watcher_map_.find(encode_key);
        ASSERT_TRUE(key_watcher_it != key_watcher_map_.end());

        auto key_watcher_map = key_watcher_it->second;
        auto watcher_it = key_watcher_map->find(watcher_id);
        ASSERT_TRUE(watcher_it != key_watcher_map->end());
        std::cout << "check add watcher map ok: session id: [" <<
                  watcher_id << "] key: [" << encode_key << "]" << std::endl;

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            Key tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys());
            if (tmp_key == encode_key && tmp_id == watcher_id) {
                std::cout << "check add watcher queue ok: session id: [" <<
                          tmp_id << "] key: [" << tmp_key << "]" << std::endl;
                break;
            }
        }
        ASSERT_TRUE(it != watcher_queue.end());
    }

    void CheckDelKeywatcher(TestWatcherPtr& w_ptr, bool is_sent_response) {
        Key encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys());
        auto watcher_id = w_ptr->GetWatcherId();

        // check key map
        auto watcher_id_it = key_map_.find(watcher_id);
        if (watcher_id_it != key_map_.end()) {
            auto watcher_key_map = watcher_id_it->second;
            auto key_it = watcher_key_map->find(encode_key);
            ASSERT_TRUE(key_it == watcher_key_map->end());
            std::cout << "check del key map ok: session id: [" <<
                      watcher_id << "] key: [" << encode_key << "]" << std::endl;
        }

        // check watcher map
        auto key_watcher_it = key_watcher_map_.find(encode_key);
        if (key_watcher_it != key_watcher_map_.end()) {
            auto key_watcher_map = key_watcher_it->second;
            auto watcher_it = key_watcher_map->find(watcher_id);
            ASSERT_TRUE(watcher_it == key_watcher_map->end());
            std::cout << "check del watcher map ok: session id: [" <<
                      watcher_id << "] key: [" << encode_key << "]" << std::endl;
        }

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            Key tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys());
            if (tmp_key == encode_key && tmp_id == watcher_id) {
                ASSERT_TRUE(w_p->IsSentResponse() == is_sent_response);
                std::cout << "check del watcher in queue: session id: [" <<
                          tmp_id << "] key: [" << tmp_key << "] is sent response: [" << w_p->IsSentResponse() << "]" << std::endl;
                break;
            }
        }
        ASSERT_TRUE(it != watcher_queue.end()); // ptr is always in queue, until queue pop
    }

};

class TestWatchServer: public WatchServer {
public:
    TestWatchServer():WatchServer(3) {}
};

TEST(TestWatcher, EncodeAndDecode) {
    std::vector<Key*> keys;
    keys.push_back(new Key("k1"));
    keys.push_back(new Key("k2"));
    keys.push_back(new Key("k10"));

    common::ProtoMessage* message = new common::ProtoMessage();

    TestWatcher w(keys, message);

    // test encode and decode key
    Key encode_key;
    w.EncodeKey(&encode_key, g_table_id, w.GetKeys());

    std::vector<Key*> decode_keys;
    w.DecodeKey(decode_keys, encode_key);
    std::cout << "0: " << *decode_keys[0] <<  std::endl;
    ASSERT_EQ(*decode_keys[0], Key("k1"));
    std::cout << "1: " << *decode_keys[1] <<  std::endl;
    ASSERT_EQ(*decode_keys[1], Key("k2"));
    std::cout << "2: " << *decode_keys[2] <<  std::endl;
    ASSERT_EQ(*decode_keys[2], Key("k10"));
}


TEST(TestWatcherSet, AddAndDelKeyWatcher) {
    std::vector<Key*> keys0;
    keys0.push_back(new Key("k0.1"));
    keys0.push_back(new Key("k0.2"));
    keys0.push_back(new Key("k0.10"));

    std::vector<Key*> keys1;
    keys1.push_back(new Key("k1.1"));
    keys1.push_back(new Key("k1.2"));
    keys1.push_back(new Key("k1.10"));

    common::ProtoMessage* msg0 = new common::ProtoMessage();
    msg0->session_id = 1;
    msg0->expire_time = getticks()+3000;

    common::ProtoMessage* msg1 = new common::ProtoMessage();
    msg1->session_id = 2;
    msg1->expire_time = getticks()+3000;

    TestWatcherPtr w_ptr0 = std::make_shared<TestWatcher>(keys0, msg0);
    TestWatcherPtr w_ptr1 = std::make_shared<TestWatcher>(keys0, msg1);
    TestWatcherPtr w_ptr2 = std::make_shared<TestWatcher>(keys1, msg1);

    std::string encode_key0;
    w_ptr0->EncodeKey(&encode_key0, g_table_id, w_ptr0->GetKeys());
    std::string encode_key1;
    w_ptr1->EncodeKey(&encode_key1, g_table_id, w_ptr1->GetKeys());
    std::string encode_key2;
    w_ptr2->EncodeKey(&encode_key2, g_table_id, w_ptr2->GetKeys());

    TestWatcherSet ws;
    WatcherPtr w_p0 = std::static_pointer_cast<Watcher>(w_ptr0);
    w_p0->SetWatcherId(0);
    WatcherPtr w_p1 = std::static_pointer_cast<Watcher>(w_ptr1);
    w_p1->SetWatcherId(1);
    WatcherPtr w_p2 = std::static_pointer_cast<Watcher>(w_ptr2);
    w_p2->SetWatcherId(2);

    // test add
    ws.AddKeyWatcher(encode_key0, w_p0);
    ws.CheckAddKeyWatcher(w_ptr0);

    ws.AddKeyWatcher(encode_key1, w_p1);
    ws.CheckAddKeyWatcher(w_ptr1);

    ws.AddKeyWatcher(encode_key2, w_p2);
    ws.CheckAddKeyWatcher(w_ptr2);

    // test del
    ws.DelKeyWatcher(encode_key0, w_p0->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr0, false);

    ws.DelKeyWatcher(encode_key1, w_p1->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr1, false);

    ws.DelKeyWatcher(encode_key2, w_p2->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr2, false);
}

TEST(TestWatchServer, GetWatcherSet) {
    TestWatchServer test_watch_server;
    ASSERT_TRUE(test_watch_server.GetWatcherSet_(Key("a")) != nullptr);
    ASSERT_TRUE(test_watch_server.GetWatcherSet_(Key("b")) != nullptr);
    ASSERT_TRUE(test_watch_server.GetWatcherSet_(Key("c")) != nullptr);
}

} // namespace
