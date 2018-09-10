#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include <common/ds_config.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/mock/raft_server_mock.h"
#include "helper/mock/socket_session_mock.h"
#include "helper/table.h"
#include "range/range.h"

#include "watch/watcher.h"
#include "common/socket_base.h"
#include "lk_queue/lk_queue.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);
char level[8] = "debug";

int main(int argc, char *argv[]) {
    if(argc > 1)
        strcpy(level, argv[1]);

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;

class SocketBaseMock: public common::SocketBase {

public:
    virtual int Send(response_buff_t *response) {
        FLOG_DEBUG("Send mock...%s", response->buff);
        return 0;
    }
};


std::string  DecodeSingleKey(const int16_t grpFlag, const std::string &encodeBuf) {
    std::vector<std::string *> vec;
    std::string key("");
    auto buf = new std::string(encodeBuf);

    watch::Watcher watcher(1, vec);
    watcher.DecodeKey(vec, encodeBuf);

        if(grpFlag) {
            for(auto it:vec) {
                key.append(*it);
            }
        } else {
            key.assign(*vec[0]);
        }

    
   //     FLOG_DEBUG("DecodeWatchKey exception(%d), %s", int(vec.size()), EncodeToHexString(*buf).c_str());
    

    if(vec.size() > 0 && key.empty())
        key.assign(*vec[0]);

    FLOG_DEBUG("DecodeKey: %s", key.c_str());
    return key;
}

class WatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());

        sf_socket_thread_config_t config;
        sf_socket_status_t status = {0};

        socket_.Init(&config, &status);

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->socket_session = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        ds_config.range_config.recover_concurrency = 1;
        range_server_->Init(context_);
        now = getticks();
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
    int64_t now;
    SocketBaseMock socket_;
};

metapb::Range *genRange1() {
    //watch::Watcher watcher;
    auto meta = new metapb::Range;
    
    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01003"), k2("01004");

    keys.push_back(&k1);
    watch::Watcher watcher1(1, keys);
    watcher1.EncodeKey(&keyStart, 1, keys);

    keys.clear();
    keys.push_back(&k2);
    watch::Watcher watcher2(1, keys);
    watcher2.EncodeKey(&keyEnd, 1, keys);

    meta->set_id(1);
    //meta->set_start_key("01003");
    //meta->set_end_key("01004");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = sharkstore::test::helper::CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

metapb::Range *genRange2() {
    //watch::Watcher watcher;
    auto meta = new metapb::Range;

    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01004"), k2("01005");

    keys.push_back(&k1);
    watch::Watcher watcher1(1, keys);
    watcher1.EncodeKey(&keyStart, 1, keys);

    keys.clear();
    keys.push_back(&k2);
    watch::Watcher watcher2(1, keys);
    watcher2.EncodeKey(&keyEnd, 1, keys);

    meta->set_id(2);
    //meta->set_start_key("01004");
    //meta->set_end_key("01005");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = sharkstore::test::helper::CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

template<typename T>
class ConcurrentQueueTests {

public:
    ConcurrentQueueTests() {
        lk_queue = new_lk_queue();
    }

    ~ConcurrentQueueTests() {
        delete_lk_queue(lk_queue);
    }

    int enqueue(int64_t cnt) {
        for(int i=0; i++ < cnt;) {
            char szKey2[128] = {0};
            sprintf(szKey2, "01004001%d", i);
            std::string *element = new std::string(szKey2);

            lk_queue_push(lk_queue, element);
        }
        return 0;
    }

    T* dequeue() {
        return static_cast<T *>(lk_queue_pop(lk_queue));
    }

    void print(int64_t cnt) {
        //lk_queue->size_approx()
        //std::cout << lk_queue->index_t << std::endl;

        for(int i=0; i++<cnt;) {
            auto ele = dequeue();
            FLOG_DEBUG("%d---%s", i, static_cast<T *>(ele)->c_str());
        }
    }

public:
    lock_free_queue_t *lk_queue = nullptr;
};

TEST_F(WatchTest, test_lk_queue) {

    ConcurrentQueueTests<std::string> test;

    int64_t size(10000000);

    int64_t btime1 = getticks();
    FLOG_INFO("%" PRId64, btime1);
    test.enqueue(size);
    int64_t btime2 = getticks();
    FLOG_INFO("%" PRId64, btime2);
    test.print(size);
    int64_t etime = getticks();
    FLOG_INFO("%" PRId64, etime);

    FLOG_WARN("total:%" PRIi64 "/ms enqueue:%" PRIi64 "/ms dequeue:%" PRIi64 "/ms",
              size/((etime - btime1)/1), size/((btime2 - btime1)/1), size/ ((etime - btime2)/1));


}




