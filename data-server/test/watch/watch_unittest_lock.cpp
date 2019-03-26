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

#include "watch/watcher.h"
#include "test_public_funcs.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);

char level[8] = "debug";

int main(int argc, char *argv[]) {
    if(argc > 1)
        strcpy(level, argv[1]);

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

metapb::Range *genRange2();
metapb::Range *genRange1(); 

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

class LockTest : public ::testing::Test {
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
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
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

    void justDel(const int16_t &rangeId, const std::string &key1, const std::string &value, bool prefix = false)
    {
        FLOG_DEBUG("justDel...range:%d key1:%s  ", rangeId, key1.c_str());

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
        req.mutable_req()->mutable_kv()->set_version(1); 
        req.mutable_req()->set_prefix(prefix); 

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchDel);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        auto msgId(rand());
        FLOG_DEBUG("msg_id:%" PRId32, msgId);
        rpc.first->msg->head.msg_id = msgId; 

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchDeleteResponse resp;
        auto s = rpc.second->Get(resp); 

        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str()); 
        ASSERT_FALSE(resp.header().has_error());

        // end test watch_delete 
    }

    void LockGet(const int16_t &rangeId, const std::string key, const std::string& val, const int32_t& cnt)
    {
        FLOG_DEBUG("LockGet...range:%d key1: %s ", rangeId, key.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        kvrpcpb::DsLockGetRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->set_key(key);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncLockGet);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros(); 

        range_server_->DealTask(std::move(rpc.first));

        kvrpcpb::DsLockGetResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        // ASSERT_FALSE(resp.header().has_error()); 
        FLOG_DEBUG(">>>LockGet RESP:%s", resp.DebugString().c_str());

        if(!cnt) {
            ASSERT_TRUE(resp.header().has_error());
            ASSERT_TRUE(resp.resp().code() == 1);
        } else {
            ASSERT_TRUE(resp.resp().code() == 0);
        }
    }

    void Lock(const int16_t &rangeId, const std::string &key,const std::string &value, const std::string& id, bool result = true)
    {
        FLOG_DEBUG("Lock...range:%d key1:%s  value:%s", rangeId, key.c_str(), value.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1 ;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;


        kvrpcpb::DsLockRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        std::string by("localhost:80"); 
        req.mutable_req()->set_key(key);
        req.mutable_req()->mutable_value()->set_delete_time(1000);
        req.mutable_req()->mutable_value()->set_by(by);
        req.mutable_req()->mutable_value()->set_id(id);
        req.mutable_req()->mutable_value()->set_value(value); 

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncLock);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros();
        srand(time(NULL));
        auto msgId(rand());
        FLOG_DEBUG("msg_id:%" PRId32, msgId);
        rpc.first->msg->head.msg_id = msgId;

        range_server_->DealTask(std::move(rpc.first));

        kvrpcpb::DsLockResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        FLOG_DEBUG("Lock first response: %s", resp.DebugString().c_str());

        if(result)
            ASSERT_TRUE(resp.resp().code() == 0);
        else
            ASSERT_TRUE(resp.resp().code() == 2);

        return;
    }

    void LockWatch(const int16_t &rangeId, const std::string key, bool result = true)
    {
        FLOG_DEBUG("justWatch...range:%d key:%s ", rangeId, key.c_str());
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;

        watchpb::DsWatchRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key);
        //req.mutable_req()->mutable_kv()->set_version(1);
        req.mutable_req()->set_longpull(5000);
        ///////////////////////////////////////////////
        //传入大版本号，用于让watcher添加成功
        req.mutable_req()->set_startversion(0);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncLockWatch);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813; 

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsWatchResponse resp;
        auto s = rpc.second->Get(resp);
        
//        ASSERT_TRUE(s.ok()) << s.ToString();
//        ASSERT_FALSE(resp.header().has_error()); 

        if(result)
            ASSERT_FALSE(resp.header().has_error());
        else {
            ASSERT_TRUE(resp.resp().code() == 1);
        }
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};
/*
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

//    peer = meta->add_peers();
//    peer->set_id(2);
//    peer->set_node_id(2);

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

    return meta;
}
*/

TEST_F(LockTest, watch_lock_get) {
    //not exist
    LockGet(1, "01003001", "", 0);

    //put a lock,then get
    Lock(1,"0100301", "val", "lock_1");
    LockGet(1, "0100301", "val", 1);

    //lock by same id, expect success
    Lock(1,"0100301", "val", "lock_1", true);

    //put a lock, already exists
    Lock(1,"0100301", "val", "lock_2", false);
    //expect add watcher success
    LockWatch(1, "0100301");
    LockWatch(1, "0100301");
    LockWatch(1, "0100301");

    //delete lock and notify watcher
    justDel(1, "0100301", "val", true);

    FLOG_INFO("sleep 10 secs, wait for watcher timeout queue pop.");
    //wait for notify
    sleep(10);

    //ok  参数１　代表期望能查询到数据
    LockGet(1, "0100301", "val", 0);

    //lock is not exist, so watch fail.
    LockWatch(1, "0100301", false);

    //Lock success
    Lock(1,"0100301", "val", "lock_3", true);

}

TEST_F(LockTest, watch_lock_expire) {
    //not exist
    LockGet(1, "01003001", "", 0);

    //put a lock,then get
    Lock(1,"0100301", "val", "lock_1");
    LockGet(1, "0100301", "val", 1);

    //lock by same id, expect success
    Lock(1,"0100301", "val", "lock_1", true);

    FLOG_INFO("sleep 5 secs, wait for lock expired.");
    sleep(5);
    //put a lock, already exists
    Lock(1,"0100301", "val", "lock_2", true);
    //LockWatch(1, "0100301");
    //justDel(1, "0100301", "val", true);

    //ok  参数１　代表期望能查询到数据
    LockGet(1, "0100301", "val", 1);
    LockWatch(1, "0100301", true);

}

