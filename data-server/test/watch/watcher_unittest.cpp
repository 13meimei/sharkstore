#include <gtest/gtest.h>

#define private public
#include "watch/watcher.h"
#include "watch/watcher_set.h"
#include "frame/sf_util.h"

int main(int argc, char* argv[]) {
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

void TestWatcher::Send(google::protobuf::Message* resp) {
    std::lock_guard<std::mutex> lock(send_lock_);
    if (sent_response_flag) {
        return;
    }
    sent_response_flag = true;
}

class TestWatcherSet: public WatcherSet {
public:
    void CheckAddKeyWatcher(WatcherPtr& w) {

    }
    void CheckDelKeywatcher(WatcherPtr& w) {

    }

};

typedef std::shared_ptr<TestWatcher> TestWatcherPtr;

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
    std::vector<Key*> keys;
    keys.push_back(new Key("k1"));
    keys.push_back(new Key("k2"));
    keys.push_back(new Key("k10"));

    common::ProtoMessage* msg = new common::ProtoMessage();
    msg->session_id = 1;
    msg->begin_time = getticks();
    msg->expire_time = getticks()+3000;
    msg->msg_id = 10;

    TestWatcherPtr w_ptr = std::make_shared<TestWatcher>(keys, msg);

    std::string encode_key;
    w_ptr->EncodeKey(&encode_key, g_table_id, keys);

    TestWatcherSet ws;
    WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
    ws.AddKeyWatcher(encode_key, w_p);
    ws.CheckAddKeyWatcher(w_p);
}

}
