﻿#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/mock/raft_server_mock.h"
#include "helper/mock/rpc_request_mock.h"

#include "range/range.h"

#include "watch/watcher.h"
#include "watch/watcher_set.h"
#include "test_public_funcs.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);
extern metapb::Range *genRange2();
extern metapb::Range *genRange1();

char level[8] = "WARN";


int main(int argc, char *argv[]) {
    if(argc > 1)
        strcpy(level, argv[1]);

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;
using namespace sharkstore::test::mock; 

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

        strcpy(ds_config.engine_config.name, "rocksdb");
        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(NowMilliSeconds()).c_str());
        ds_config.range_config.recover_concurrency = 10; 
        ds_config.watch_config.watcher_thread_priority = 23; 

        range_server_ = new server::RangeServer; 
        context_ = new server::ContextServer; 
        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);

        {
            // begin test create range
            schpb::CreateRangeRequest req;
            req.set_allocated_range(genRange1());

            auto rpc = NewMockRPCRequest(req);

            range_server_->CreateRange(*rpc.first);
            ASSERT_FALSE(range_server_->ranges_.empty());

            ASSERT_TRUE(range_server_->Find(1) != nullptr);

            std::vector<metapb::Range> metas;
            auto ret = range_server_->meta_store_->GetAllRange(&metas);

            ASSERT_TRUE(metas.size() == 1) << metas.size();
            // end test create range
        }

        {
            // begin test create range
            schpb::CreateRangeRequest req;
            req.set_allocated_range(genRange2());

            auto rpc = NewMockRPCRequest(req);

            range_server_->CreateRange(*rpc.first);
            ASSERT_FALSE(range_server_->ranges_.empty());

            ASSERT_TRUE(range_server_->Find(2) != nullptr);

            std::vector<metapb::Range> metas;
            auto ret = range_server_->meta_store_->GetAllRange(&metas);

            ASSERT_TRUE(metas.size() == 2) << metas.size();
            // end test create range
        }
    }

    void TearDown() override {
        delete context_->range_server;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
        if (strlen(ds_config.rocksdb_config.path) > 0) {
            RemoveDirAll(ds_config.rocksdb_config.path);
        }
    }

    void justPut(const int16_t &rangeId, const std::string &key1, const std::string &key2,const std::string &value)
    {
        FLOG_DEBUG("justPut...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , value.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        watchpb::DsKvWatchPutRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2);
        req.mutable_req()->mutable_kv()->set_value(value); 

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchPut);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros();

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchPutResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        return;
    }

    void justDel(const int16_t &rangeId, const std::string &key1, const std::string &key2, const std::string &value, bool prefix = false)
    {
        FLOG_DEBUG("justDel...range:%d key1:%s  key2:%s", rangeId, key1.c_str(), key2.c_str() );

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        watchpb::DsKvWatchDeleteRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2); 
        req.mutable_req()->mutable_kv()->set_version(1); 
        req.mutable_req()->set_prefix(prefix);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchDel);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813; 

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchDeleteResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        ASSERT_FALSE(resp.header().has_error());
        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str()); 

        // end test watch_delete 
    }

    void justGet(const int16_t &rangeId, const std::string key1, const std::string &key2, const std::string& val, const int32_t& cnt, bool prefix = false)
    {
        FLOG_DEBUG("justGet...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , val.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        watchpb::DsKvWatchGetMultiRequest req; 
        req.set_prefix(prefix);
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1); 
        req.mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_kv()->add_key(key2);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncPureGet);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros();

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchGetMultiResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        FLOG_DEBUG(">>>PureGet RESP:%s", resp.DebugString().c_str());
        ASSERT_TRUE(resp.code() == 0);
        EXPECT_TRUE(resp.kvs_size() == cnt);

        if (cnt && resp.kvs_size()) {
            for(auto i = 0; i<cnt; i++)
                EXPECT_TRUE(resp.kvs(i).value() == val);
        }
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};


TEST_F(WatchTest, pure_get_single) {

    metapb::Range* rng = new metapb::Range;
    range_server_->meta_store_->GetRange(1, rng);
    FLOG_DEBUG("RANGE1  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());

    range_server_->meta_store_->GetRange(2, rng);
    FLOG_DEBUG("RANGE2  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());


    FLOG_DEBUG("pure_get begin...");
    {
        justPut(2, "01004001", "", "01004001:value");
        justGet(2, "01004001", "", "01004001:value", 1);
    }

}


TEST_F(WatchTest, pure_get_group) {

    FLOG_DEBUG("pure_get group begin...");
    {
        std::string value("01003001:value");

        justPut(1, "01003001", "01003002", "01003001:value");
        justGet(1, "01003001", "01003002", "01003001:value", true);

        //查询不存在的元素
        justDel(1, "01003001", "", "", true);
        justGet(1, "01003001", "", "01003001:value", 0, true);

        //放置３个元素，判断返回元素个数
        justPut(1, "01003001", "0100300101", value);
        justPut(1, "01003001", "0100300102", value);
        justPut(1, "01003001", "0100300103", value);
        justGet(1, "01003001", "", "01003001:value", 3, true);

    }

}

//测试优先级队列元素是否按失效时间排序，先失效元素在对头
TEST_F(WatchTest, test_priority_queue) {
    {
        #define QUEUE_CAPACITY 100

        watch::PriorityQueue<watch::WatcherPtr>   watcher_queue;

        int64_t cnt(QUEUE_CAPACITY);
        std::string keys("");
        char tmp[10] = {0};
        std::vector<watch::WatcherKey*> vecKey;

        while (cnt > 0) {

            int64_t expireTime = NowMicros() + 5000000;
            sprintf(tmp, "a_%ld", cnt);
            keys.assign(tmp);

            vecKey.clear();
            vecKey.push_back(new std::string(keys));

            schpb::CreateRangeRequest req;
            req.set_allocated_range(genRange1()); 
            // begin test watch_get (key empty) 
            auto rpc = NewMockRPCRequest(req);
            range_server_->CreateRange(*rpc.first);
            ASSERT_FALSE(range_server_->ranges_.empty());

            rpc.first->expire_time = NowMilliSeconds() + 5000;
            rpc.first->begin_time = NowMicros();
            rpc.first->msg->head.msg_id = cnt + 100;

            auto w_ptr = std::make_shared<watch::Watcher>(watch::WATCH_KEY, 1, vecKey, 0, expireTime, std::move(rpc.first));

            watcher_queue.push(w_ptr);
            cnt--;
        }

        ASSERT_TRUE(watcher_queue.size() == QUEUE_CAPACITY);

        int64_t oldTime(0);
        while(!watcher_queue.empty()) {
            int64_t currTime(0);
            if(0== oldTime) {
                oldTime = watcher_queue.top()->expire_time_;
            } else {
                currTime = watcher_queue.top()->expire_time_;
                ASSERT_TRUE(currTime >= oldTime);

                oldTime = currTime;
            }
            //FLOG_DEBUG("expire_time:%" PRId64, oldTime);

            watcher_queue.pop();
        } 
    } 
} 

#undef watch_get_benchmark
#ifdef watch_get_benchmark
TEST_F(WatchTest, watch_get_benchmark) {

    justDel(1, "01003001", "", "", true);
    justPut(1, "01003001", "", "03003001:value");

    int64_t count(1000000);
    int64_t bTime(NowMilliSeconds());
    for (int i = 0; i < count; i++) {
        justGet(1, "01003001", "", "03003001:value", 1, false);
    }
    int64_t endTime(NowMilliSeconds());
    FLOG_WARN("count:%" PRId64 " elapse:%" PRId64 "s average:%" PRId64 "/s",count, (endTime - bTime)/1000, count/ ((endTime - bTime)/1000));

}
#endif

/*
TEST_F(WatchTest, test_info) {
    FLOG_WARN("-------------------------------------------------------");
    FLOG_WARN("pure_get_single 单key查询");
    FLOG_WARN("pure_get_group　前缀查询,查询不存在的key");
    FLOG_WARN("test_priority_queue 超时队列元素排序效果测试");
    FLOG_WARN("-------------------------------------------------------");
}
*/
