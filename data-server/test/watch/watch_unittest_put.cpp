#include <gtest/gtest.h>
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
#include "test_public_funcs.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);

char level[8] = "warn";

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
        EXPECT_TRUE(resp.kvs_size() == cnt);

        if (cnt && resp.kvs_size()) {
            for(auto i = 0; i<cnt; i++)
                EXPECT_TRUE(resp.kvs(i).value() == val);
        }
    }

    void justPut(const int16_t &rangeId, const std::string &key1, const std::string &key2,const std::string &value)
    {
        FLOG_DEBUG("justPut...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , value.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1 ;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;

        watchpb::DsKvWatchPutRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2);
        req.mutable_req()->mutable_kv()->set_value(value);
        req.mutable_req()->mutable_kv()->set_version(99);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchPut);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros();
        srand(time(NULL));
        auto msgId(rand());
        FLOG_DEBUG("msg_id:%" PRId32, msgId);
        rpc.first->msg->head.msg_id = msgId; 

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchPutResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        FLOG_DEBUG("watch_put first response: %s", resp.DebugString().c_str());

        return;
    }

    void justWatch(const int16_t &rangeId, const std::string key1, const std::string key2, bool prefix = false)
    {
        FLOG_DEBUG("justWatch...range:%d key1:%s  key2:%s  prefix:%d", rangeId, key1.c_str(), key2.c_str(), prefix );
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;

        watchpb::DsWatchRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty()) {
            req.mutable_req()->mutable_kv()->add_key(key2);
        }
        req.mutable_req()->mutable_kv()->set_version(1);
        req.mutable_req()->set_longpull(5000);
        ///////////////////////////////////////////////
        //传入大版本号，用于让watcher添加成功
        req.mutable_req()->set_startversion(800);
        req.mutable_req()->set_prefix(prefix);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchGet);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813;

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsWatchResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
}; 

TEST_F(WatchTest, watch_put_single) {

    justPut(1, "01003001", "", "01003001:value");
    justGet(1, "01003001", "", "01003001:value", 1);

    //update
    justPut(1, "01003001", "", "0100300101:value");
    justGet(1, "01003001", "", "0100300101:value", 1);

    //another range
    justPut(2, "01004001", "", "01004001:value");
    justGet(2, "01004001", "", "01004001:value", 1);

    //other occasion,for examples:
    //not leader
    //not in range 
}

TEST_F(WatchTest, watch_put_group) {

//        // begin test watch_put group (key ok)
//        FLOG_DEBUG("watch_put group mode.");
//        metapb::Range* rng = new metapb::Range;
//        range_server_->meta_store_->GetRange(1, rng);
//        FLOG_DEBUG("RANGE1  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());
//
//        range_server_->meta_store_->GetRange(2, rng);
//        FLOG_DEBUG("RANGE2  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());

    for(auto i = 0; i < 1000; i ++) {
        char szKey2[1000] = {0};
        sprintf(szKey2, "01004001%d", i);
        std::string key2(szKey2);
        justPut(2, "0100400101", key2, "01004001:value");
    }

    justGet(2, "0100400101", "", "01004001:value", 1000, true);

//    sleep(5);
}

TEST_F(WatchTest, DISABLED_watch_put_benchmark) {

    FLOG_DEBUG("watch_put single mode.");

    int64_t count(1000000);
    int64_t bTime(NowMilliSeconds());
    for (int i = 0; i < count; i++) {
        justPut(1, "01003001", "", "01003001:value");
    }
    int64_t endTime(NowMilliSeconds());
    FLOG_WARN("count:%" PRId64 " elapse:%" PRId64 "s average:%" PRId64 "/s",count, (endTime - bTime)/1000, count/ ((endTime - bTime)/1000));

}
