#include <gtest/gtest.h>
#include "helper/cpp_permission.h" 
#include "helper/mock/rpc_request_mock.h"

#include "base/util.h"
#include "base/status.h"
#include "fastcommon/logger.h"

#include "watch/watch.h"
#include "watch/watcher.h"
#include "watch/watcher_set.h"
#include "watch/watch_server.h" 

#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"

#include "common/rpc_request.h"
#include "storage/store.h" 

#include "test_public_funcs.h"

int main(int argc, char* argv[]) {
    log_init2();

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
using namespace sharkstore;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::watch;
using namespace sharkstore::test::mock; 

enum TestCode {
    TEST_OK = 0,
    TEST_ERR,
    TEST_TIMEOUT,
    TEST_UNEXPECTED,
}; 

const uint64_t g_table_id = 123;

class TestWatcher: public Watcher {
public:
    TestWatcher() = delete;
    TestWatcher(const std::vector<WatcherKey*>& keys, RPCRequestPtr message): Watcher( g_table_id, keys, 0, NowMicros()+1000000, std::move(message)) {
    }
    ~TestWatcher() = default;

    void Send(google::protobuf::Message* resp) override;
};
typedef std::shared_ptr<TestWatcher> TestWatcherPtr;

void TestWatcher::Send(google::protobuf::Message* resp) {
    (void) resp; // not used
    std::lock_guard<std::mutex> lock(send_lock_);
    if (sent_response_flag) {
        return; 
    }

    message_->Reply(*resp);

    sent_response_flag = true;
}

class TestWatcherSet: public WatcherSet {
public:
    void CheckAddKeyWatcher(TestWatcherPtr& w_ptr) {
        WatcherKey encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys(false));
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

        auto key_watcher_map = key_watcher_it->second->mapKeyWatcher;
        auto watcher_it = key_watcher_map.find(watcher_id);
        ASSERT_TRUE(watcher_it != key_watcher_map.end());
        std::cout << "check add watcher map ok: session id: [" <<
                  watcher_id << "] key: [" << encode_key << "]" << std::endl;

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            WatcherKey tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys(false));
            if (tmp_key == encode_key && tmp_id == watcher_id) {
                std::cout << "check add watcher queue ok: session id: [" <<
                          tmp_id << "] key: [" << tmp_key << "]" << std::endl;
                break;
            }
        }
        ASSERT_TRUE(it != watcher_queue.end());
    }

    void CheckDelKeywatcher(TestWatcherPtr& w_ptr, bool is_sent_response) {
        WatcherKey encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys(false));
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
            auto key_watcher_map = key_watcher_it->second->mapKeyWatcher;
            auto watcher_it = key_watcher_map.find(watcher_id);
            ASSERT_TRUE(watcher_it == key_watcher_map.end());
            std::cout << "check del watcher map ok: session id: [" <<
                      watcher_id << "] key: [" << encode_key << "]" << std::endl;
        }

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            WatcherKey tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys(false));
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
    std::vector<WatcherKey*> keys;
    keys.push_back(new WatcherKey("k1"));
    keys.push_back(new WatcherKey("k2"));
    keys.push_back(new WatcherKey("k10"));

    schpb::CreateRangeRequest req;
    req.set_allocated_range(genRange1());

    auto rpc = NewMockRPCRequest(req); 
    TestWatcher w(keys, std::move(rpc.first));
    
    // test encode and decode key
    WatcherKey encode_key;
    w.EncodeKey(&encode_key, g_table_id, w.GetKeys(false));

    std::vector<WatcherKey*> decode_keys;
    w.DecodeKey(decode_keys, encode_key);
    std::cout << "0: " << *decode_keys[0] <<  std::endl;
    ASSERT_EQ(*decode_keys[0], WatcherKey("k1"));
    std::cout << "1: " << *decode_keys[1] <<  std::endl;
    ASSERT_EQ(*decode_keys[1], WatcherKey("k2"));
    std::cout << "2: " << *decode_keys[2] <<  std::endl;
    ASSERT_EQ(*decode_keys[2], WatcherKey("k10"));
}


TEST(TestWatcherSet, AddAndDelKeyWatcher) {
    std::vector<WatcherKey*> keys0;
    keys0.push_back(new WatcherKey("k0.1"));
    keys0.push_back(new WatcherKey("k0.2"));
    keys0.push_back(new WatcherKey("k0.10"));

    std::vector<WatcherKey*> keys1;
    keys1.push_back(new WatcherKey("k1.1"));
    keys1.push_back(new WatcherKey("k1.2"));
    keys1.push_back(new WatcherKey("k1.10"));

    //rpc1
    schpb::CreateRangeRequest req1;
    req1.set_allocated_range(genRange1());

    auto rpc1 = NewMockRPCRequest(req1); 
    
    // rpc2
    schpb::CreateRangeRequest req2;
    req2.set_allocated_range(genRange2());

    auto rpc2 = NewMockRPCRequest(req2); 

    // rpc3
    schpb::CreateRangeRequest req3;
    req3.set_allocated_range(genRange2());

    auto rpc3 = NewMockRPCRequest(req3); 

    TestWatcherPtr w_ptr0 = std::make_shared<TestWatcher>(keys0, std::move(rpc1.first));
    TestWatcherPtr w_ptr1 = std::make_shared<TestWatcher>(keys0, std::move(rpc2.first));
    TestWatcherPtr w_ptr2 = std::make_shared<TestWatcher>(keys1, std::move(rpc3.first));

    std::string encode_key0;
    w_ptr0->EncodeKey(&encode_key0, g_table_id, w_ptr0->GetKeys(false));
    std::string encode_key1;
    w_ptr1->EncodeKey(&encode_key1, g_table_id, w_ptr1->GetKeys(false));
    std::string encode_key2;
    w_ptr2->EncodeKey(&encode_key2, g_table_id, w_ptr2->GetKeys(false));

    TestWatcherSet ws;
    WatcherPtr w_p0 = std::static_pointer_cast<Watcher>(w_ptr0);
    w_p0->SetWatcherId(0);
    WatcherPtr w_p1 = std::static_pointer_cast<Watcher>(w_ptr1);
    w_p1->SetWatcherId(1);
    WatcherPtr w_p2 = std::static_pointer_cast<Watcher>(w_ptr2);
    w_p2->SetWatcherId(2);

    // test add
    ws.AddKeyWatcher(encode_key0, w_p0, nullptr);
    ws.CheckAddKeyWatcher(w_ptr0);

    ws.AddKeyWatcher(encode_key1, w_p1, nullptr);
    ws.CheckAddKeyWatcher(w_ptr1);

    ws.AddKeyWatcher(encode_key2, w_p2, nullptr);
    ws.CheckAddKeyWatcher(w_ptr2);

    // test del
    ws.DelKeyWatcher(encode_key0, w_p0->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr0, false);

    ws.DelKeyWatcher(encode_key1, w_p1->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr1, false);

    ws.DelKeyWatcher(encode_key2, w_p2->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr2, false);
}

TEST(TestWatchServer, SimulateInteractive) {
    TestWatchServer server;

    std::vector<WatcherKey *> keys0;
    keys0.push_back(new WatcherKey("k0.1"));
    keys0.push_back(new WatcherKey("k0.2"));
    keys0.push_back(new WatcherKey("k0.10"));

    { 
        watchpb::DsWatchRequest req; 
        req.mutable_header()->set_range_id(g_table_id);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->set_longpull(5000);
        req.mutable_req()->set_startversion(0);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncLockWatch);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813; 

        auto w_ptr0 = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
        WatcherPtr w_p0 = std::static_pointer_cast<Watcher>(w_ptr0);
        server.AddKeyWatcher(w_p0, nullptr); 
    }

    {
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequest(req);

        //auto conn = new TestWatchConnection(5000, "hello", "timeout");

        auto w_ptr = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr); 

    }

    {
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequest(req); 

        auto w_ptr = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr);

        sleep(1);
    }

    {

        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequest(req); 

        // auto conn = new TestWatchConnection(5000, "hello", "world");
        auto w_ptr = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr);

        sleep(1);

//        rpc.send.get();

//        conn->ServerWrite(); // simulate server response
//        ASSERT_TRUE(conn->ClientRead() == TEST_UNEXPECTED);
    }

    {
        // test concurrent timeout response
        int i;
        int N = 1000;
        int TEST_TIMES = 3;
        std::thread th[N];
        for (auto j = 0; j < TEST_TIMES; j++) {
            for (i = 0; i < N; i++) {
                th[i] = std::thread([&]() {
                    // auto conn = new TestWatchConnection(5000, "hello", "timeout"); // read timeout 5s
                
                    watchpb::DsWatchRequest req; 
                    req.mutable_header()->set_range_id(g_table_id);
                    req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
                    req.mutable_header()->mutable_range_epoch()->set_version(1); 
                    req.mutable_req()->mutable_kv()->add_key("hello");
                    req.mutable_req()->set_longpull(5000);
                    //传入大版本号，用于让watcher添加成功
                    req.mutable_req()->set_startversion(0);

                    auto rpc = NewMockRPCRequest(req, funcpb::kFuncLockWatch);
                    rpc.first->expire_time = NowMilliSeconds() + 3000;
                    rpc.first->begin_time = NowMicros();
                    rpc.first->msg->head.msg_id = 20180813; 

                    auto w_ptr = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
                    WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
                    server.AddKeyWatcher(w_p, nullptr);
                });
            }
            for (i = 0; i < N; i++) {
                th[i].join();
            }
        }

        for (auto j = 0; j < TEST_TIMES; j++) {
            for (i = 0; i < N; i++) {
                th[i] = std::thread([&]() {

                    // auto conn = new TestWatchConnection(5000); // read timeout 5s
                    schpb::CreateRangeRequest req;
                    req.set_allocated_range(genRange1());

                    auto rpc = NewMockRPCRequest(req);

                    auto w_ptr = std::make_shared<TestWatcher>(keys0, std::move(rpc.first));
                    WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
                    server.AddKeyWatcher(w_p, nullptr);

                    sleep(1);

//                     conn->ServerWrite(); // simulate server response 
//                    ASSERT_TRUE(conn->ClientRead() == TEST_OK);
                });
            }
            for (i = 0; i < N; i++) {
                th[i].join();
            }
        }
    }
}

} // namespace
