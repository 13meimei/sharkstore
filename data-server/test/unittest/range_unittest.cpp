#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "server/persist_server.h"
#include "storage/store.h"
#include "raft/src/impl/server_impl.h"

#include "helper/table.h"
//#include "helper/mock/raft_server_mock.h"
#include "helper/mock/rpc_request_wait_mock.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

//char level[8] = "warn";
char level[8] = "debug";

using namespace sharkstore;
using namespace sharkstore::test::helper;
using namespace sharkstore::test::mock;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::server;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;
using namespace sharkstore::raft;
using namespace sharkstore::raft::impl;

class RangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        init_config(ds_config);
        
        context_ = new server::ContextServer; 
        context_->node_id = 1;
        range_server_ = new server::RangeServer; 
        context_->run_status = new server::RunStatus;
        context_->persist_run_status = new server::RunStatus;
        context_->range_server = range_server_;

       
        raft::SetLogger(new RaftLogger());
        raft::RaftServerOptions ops;
        ops.node_id = context_->node_id;
        ops.election_tick = 2;
        ops.tick_interval = std::chrono::milliseconds(100);
        ops.consensus_threads_num = static_cast<uint8_t>(ds_config.raft_config.consensus_threads);
        ops.consensus_queue_capacity = ds_config.raft_config.consensus_queue;
        ops.apply_threads_num = static_cast<uint8_t>(ds_config.raft_config.apply_threads);
        ops.apply_queue_capacity = ds_config.raft_config.apply_queue;
        ops.tick_interval = std::chrono::milliseconds(ds_config.raft_config.tick_interval_ms);
        ops.max_size_per_msg = ds_config.raft_config.max_msg_size; 

        ops.apply_in_place = true;
        ops.enable_pre_vote = false;

        ops.transport_options.use_inprocess_transport = true;
        ops.transport_options.listen_port = static_cast<uint16_t>(ds_config.raft_config.port);
        ops.transport_options.send_io_threads = ds_config.raft_config.transport_send_threads;
        ops.transport_options.recv_io_threads = ds_config.raft_config.transport_recv_threads;
//        ops.transport_options.resolver = std::make_shared<NodeAddress>(context_->master_worker);
        
        ops.snapshot_options.max_send_concurrency = 1;   
        ops.snapshot_options.max_apply_concurrency = 1;   
    

        auto rs = raft::CreateRaftServer(ops);
        context_->raft_server = rs.release();
        auto s = context_->raft_server->Start();
        ASSERT_TRUE(s.ok()) << "raft_server start error.";

        server::PersistOptions opt;
        opt.thread_num = ds_config.persist_config.persist_threads;
        opt.delay_count = ds_config.persist_config.persist_delay_size;
        opt.queue_capacity = ds_config.persist_config.persist_queue_size; 

        auto sp = server::CreatePersistServer(opt);
        context_->persist_server = sp.release(); 

        ASSERT_EQ(range_server_->Init(context_), 0) << "RangeServer Init error."; 
        ASSERT_EQ(context_->persist_server->Init(context_), 0) << "PersistServer Init error";

        context_->persist_server->Start();
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        if (context_->range_server != nullptr) {
            context_->range_server->Stop();
            delete context_->range_server;
            context_->range_server = nullptr;
        }
        if (context_->raft_server != nullptr)  {
            context_->raft_server->Stop();
            delete context_->raft_server;
            context_->raft_server = nullptr;
        }

        if (context_->run_status != nullptr)  {
            delete context_->run_status;
            context_->run_status = nullptr;
        }
        
        if (context_->persist_run_status != nullptr) {
            delete context_->persist_run_status;
            context_->persist_run_status = nullptr; 
        }
        
        if (context_->persist_server != nullptr) {
            context_->persist_server->Stop();
            delete context_->persist_server;
        }
        
        if (context_ != nullptr) {
            delete context_;
            context_ = nullptr; 
        }

        FLOG_DEBUG("TearDown...");
    }

    Status testRawPut(const kvrpcpb::DsKvRawPutRequest& req, kvrpcpb::DsKvRawPutResponse& resp) {
        auto rpc = NewMockRPCRequestWait(req, funcpb::kFuncRawPut);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

    Status testRawGet(const kvrpcpb::DsKvRawGetRequest& req, kvrpcpb::DsKvRawGetResponse& resp) {
        auto rpc = NewMockRPCRequestWait(req, funcpb::kFuncRawGet);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

    Status testRawDelete(const kvrpcpb::DsKvRawDeleteRequest& req, kvrpcpb::DsKvRawDeleteResponse& resp) {
        auto rpc = NewMockRPCRequestWait(req, funcpb::kFuncRawDelete);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }


    void init_config(ds_config_t & ds)
    {
        ds.fast_worker_num = 1;
        ds.slow_worker_num = 2;
        ds.hb_config.master_num = 0;
        ds.hb_config.master_host = nullptr;

        auto ti = NowMilliSeconds();
        auto str_path = "/tmp/sharkstore_ds_store_test_" + std::to_string(NowMilliSeconds());
        auto str_path_store = str_path + "/data";
        strcpy(ds.rocksdb_config.path, str_path_store.c_str());
    //    ds.rocksdb_config.block_cache_size = 1024*1024*1024;
    //    ds.rocksdb_config.row_cache_size = 0*1024*1024;
    //    ds.rocksdb_config.block_size = 16*1024*1024;
    //    ds.rocksdb_config.max_open_files =  0;
    //    ds.rocksdb_config.bytes_per_sync = 1*1024*1024;
    //    ds.rocksdb_config.write_buffer_size = 512*1024*1024;
    //    ds.rocksdb_config.max_write_buffer_number = 16 ;
    //    ds.rocksdb_config.min_write_buffer_number_to_merge = 1;
    //    ds.rocksdb_config.max_bytes_for_level_base = 512*1024*1024;
    //    ds.rocksdb_config.max_bytes_for_level_multiplier = 10 
    //    ds.rocksdb_config.target_file_size_base = 128*1024*1024; 
    //    ds.rocksdb_config.target_file_size_multiplier = 1;
    //    ds.rocksdb_config.max_background_flushes = 1;
    //    ds.rocksdb_config.max_background_compactions = 32;
    //    ds.rocksdb_config.background_rate_limit = 100*1024*1024*1024;
    //    ds.rocksdb_config.disable_auto_compactions = true;
    //    ds.rocksdb_config.read_checksum = true ;
    //    ds.rocksdb_config.level0_file_num_compaction_trigger = 8 ;
    //    ds.rocksdb_config.level0_slowdown_writes_trigger = 40 ;
    //    ds.rocksdb_config.level0_stop_writes_trigger = 46 ;
    //    ds.rocksdb_config.disable_wal = 0 ;
    //    ds.rocksdb_config.cache_index_and_filter_blocks = 0 ;
    //    ds.rocksdb_config.compression = 10*1024;
    //    ds.rocksdb_config.storage_type = 0 ;
    //    ds.rocksdb_config.ttl = 0 ;
    //    ds.rocksdb_config.min_blob_size = 256 ;
    //    ds.rocksdb_config.enable_garbage_collection = 0;
    //    ds.rocksdb_config.blob_gc_percent = 75 ;
    //    ds.rocksdb_config.blob_compression = 1;
    //    ds.rocksdb_config.blob_cache_size = 1*1024*1024;
    //    ds.rocksdb_config.blob_file_size = 256*1024*1024;
    //    ds.rocksdb_config.blob_ttl_range = 100*1024;
    //    ds.rocksdb_config.enable_debug_log = 0;
    //    ds.rocksdb_config.enable_stats = 1;

        auto str_async_path_store = str_path + "/async";
        strcpy(ds.async_rocksdb_config.path, str_async_path_store.c_str());
    //    ds.async_rocksdb_config.block_cache_size = 1024*1024*1024;
    //    ds.async_rocksdb_config.row_cache_size = 0*1024*1024;
    //    ds.async_rocksdb_config.block_size = 16*1024*1024;
    //    ds.async_rocksdb_config.max_open_files =  0;
    //    ds.async_rocksdb_config.bytes_per_sync = 1*1024*1024;
    //    ds.async_rocksdb_config.write_buffer_size = 512*1024*1024;
    //    ds.async_rocksdb_config.max_write_buffer_number = 16 ;
    //    ds.async_rocksdb_config.min_write_buffer_number_to_merge = 1;
    //    ds.async_rocksdb_config.max_bytes_for_level_base = 512*1024*1024;
    //    ds.async_rocksdb_config.max_bytes_for_level_multiplier = 10 
    //    ds.async_rocksdb_config.target_file_size_base = 128*1024*1024; 
    //    ds.async_rocksdb_config.target_file_size_multiplier = 1;
    //    ds.async_rocksdb_config.max_background_flushes = 1;
    //    ds.async_rocksdb_config.max_background_compactions = 32;
    //    ds.async_rocksdb_config.background_rate_limit = 100*1024*1024*1024;
    //    ds.async_rocksdb_config.disable_auto_compactions = true;
    //    ds.async_rocksdb_config.read_checksum = true ;
    //    ds.async_rocksdb_config.level0_file_num_compaction_trigger = 8 ;
    //    ds.async_rocksdb_config.level0_slowdown_writes_trigger = 40 ;
    //    ds.async_rocksdb_config.level0_stop_writes_trigger = 46 ;
    //    ds.async_rocksdb_config.disable_wal = 0 ;
    //    ds.async_rocksdb_config.cache_index_and_filter_blocks = 0 ;
    //    ds.async_rocksdb_config.compression = 10*1024;
    //    ds.async_rocksdb_config.storage_type = 0 ;
    //    ds.async_rocksdb_config.ttl = 0 ;
    //    ds.async_rocksdb_config.min_blob_size = 256 ;
    //    ds.async_rocksdb_config.enable_garbage_collection = 0;
    //    ds.async_rocksdb_config.blob_gc_percent = 75 ;
    //    ds.async_rocksdb_config.blob_compression = 1;
    //    ds.async_rocksdb_config.blob_cache_size = 1*1024*1024;
    //    ds.async_rocksdb_config.blob_file_size = 256*1024*1024;
    //    ds.async_rocksdb_config.blob_ttl_range = 100*1024;
    //    ds.async_rocksdb_config.enable_debug_log = 0;
    //    ds.async_rocksdb_config.enable_stats = 1;

        ds.range_config.recover_skip_fail = 0;
        ds.range_config.recover_concurrency = 1;
        ds.range_config.check_size = 1*1024*1024;
        ds.range_config.split_size = 1*1024*1024;
        ds.range_config.max_size = 1*1024*1024;
        ds.range_config.worker_threads = 1;
        ds.range_config.access_mode= 1;

        ds.raft_config.port = 0; 
        auto str_path_raft = str_path + "/raft/"; 
        strcpy(ds.raft_config.log_path, str_path_raft.c_str());
        ds.raft_config.log_file_size = 16*1024*1024;
        ds.raft_config.max_log_files = 1000;
        ds.raft_config.allow_log_corrupt = 1;
        ds.raft_config.consensus_threads = 1;
        ds.raft_config.consensus_queue = 10000;
        ds.raft_config.apply_threads = 1;
        ds.raft_config.apply_queue = 10000;
        ds.raft_config.transport_send_threads = 1;
        ds.raft_config.transport_recv_threads = 1;
        ds.raft_config.tick_interval_ms = 5;
        ds.raft_config.max_msg_size = 10;

        
        ds.metric_config.interval =  0;

        ds.watch_config.buffer_map_size = 1;
        ds.watch_config.buffer_queue_size = 1;
        ds.watch_config.watcher_set_size = 1;
        ds.watch_config.watcher_thread_priority = 1;
        
        // sf_socket_thread_config_t manager_config;
        // sf_socket_thread_config_t worker_config;
        
//        strcpy(ds.engine_config.name, "rocksdb");
        strcpy(ds.engine_config.name, "memory");
        
        ds_config.range_config.recover_concurrency = 1;

        ds.persist_config.persist_switch = 1;
        strcpy(ds_config.persist_config.persist_type, "rocksdb");
        ds.persist_config.persist_threads = 1; 
        ds.persist_config.persist_queue_size = 10000;
        ds.persist_config.persist_delay_size = 1; 
    }


protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};

metapb::Range *genRange1() {
    auto meta = new metapb::Range;

    meta->set_id(1);
    meta->set_start_key("01003");
    meta->set_end_key("01004");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

/*    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);
*/
    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

metapb::Range *genRange2() {
    auto meta = new metapb::Range;

    meta->set_id(2);
    meta->set_start_key("01004");
    meta->set_end_key("01005");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

TEST_F(RangeTest, Range_Raw) {
    {
        // begin test create range
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequestWait(req);
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

        auto rpc = NewMockRPCRequestWait(req);
        range_server_->CreateRange(*rpc.first);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(2) != nullptr);

        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 2) << metas.size();
        // end test create range
    }

    auto r_ptr1 = context_->range_server->ranges_[1]->raft_;
    auto r_ptr2 = context_->range_server->ranges_[2]->raft_; 

    while (1) {
        usleep(1000*100);
        if (r_ptr1->IsLeader() && r_ptr2->IsLeader()) {
            break;
        } else {
            usleep(1000*100);
        } 
    }


    {
        // begin test raw_put (no leader)
        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().message();

        // end test raw_put
    }

    {
        // begin test raw_put (no leader)
        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("0100300");
        req.mutable_req()->set_value("0100300:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().message();

        // end test raw_put
    }

    {
        // begin test raw_put (no leader)
        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("010030");
        req.mutable_req()->set_value("010030:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().message();

        // end test raw_put
    }


/*
    {
        // begin test raw_put (not leader)

        // set leader
        range_server_->ranges_[1]->raft_->SetLeaderTerm(2, 1);
        range_server_->ranges_[1]->is_leader_ = false;

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_put
    }

    {
        // begin test raw_put (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        ASSERT_TRUE(resp.header().error().key_not_in_range().start_key() == "01003");
        ASSERT_TRUE(resp.header().error().key_not_in_range().end_key() == "01004");

        // end test raw_put
    }

    {
        // begin test raw_put (stale epoch)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);
        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        // end test raw_put
    }

    {
        // begin test raw_put (key empty)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("");
        req.mutable_req()->set_value("01003001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_put
    }

    {
        // begin test raw_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_put
    }

    {
        // begin test raw_put (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);
        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_put
    }

    {
        // begin test raw_get(ok)
        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01003001:value");

        // end test raw_get
    }

    {
        // begin test raw_get (ok)
        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().DebugString();
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_get (key empty)
        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test raw_get
    }

    {
        // begin test raw_get (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(0, 2);
        range_server_->ranges_[1]->is_leader_ = false;

        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test raw_get
    }

    {
        // begin test raw_get (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(2, 2);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_get
    }

    {
        // begin test raw_get (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_get
    }

    {
        // begin test raw_get (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_delete (key empty)
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test raw_delete
    }

    {
        // begin test raw_delete (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(0, 1);
        range_server_->ranges_[1]->is_leader_ = false;

        kvrpcpb::DsKvRawDeleteRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test raw_delete
    }

    {
        // begin test raw_delete (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(2, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawDeleteRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_delete
    }

    {
        // begin test raw_delete (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawDeleteRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_delete
    }

    {
        // begin test raw_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01003001:value");

        // end test raw_get
    }

    {
        // begin test raw_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawDeleteRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_delete
    }

    {
        // begin test raw_delete (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        kvrpcpb::DsKvRawDeleteRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto s = testRawDelete(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_delete
    }

    {
        // begin test raw_get(ensure raw delete)
        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        // end test raw_get
    }

    {
        // begin test raw_get (ensure raw delete)
        kvrpcpb::DsKvRawGetRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01004001");

        kvrpcpb::DsKvRawGetResponse resp;
        auto s = testRawGet(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        // end test raw_get
    }

    {
        // begin test delete range (range 1)
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto rpc = NewMockRPCRequestWait(req);
        range_server_->DeleteRange(*rpc.first);

        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (range 2)
        schpb::DeleteRangeRequest req;
        req.set_range_id(2);
        auto rpc = NewMockRPCRequestWait(req);
        range_server_->DeleteRange(*rpc.first);

        ASSERT_TRUE(range_server_->Find(2) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }
    */

}
/*
TEST_F(RawTest, TestRangeSlave) {
    {
        // begin test create range
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequestWait(req);
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

        auto rpc = NewMockRPCRequestWait(req);
        range_server_->CreateRange(*rpc.first);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(2) != nullptr);

        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 2) << metas.size();
        // end test create range
    }

    {
        // begin test raw_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        kvrpcpb::DsKvRawPutRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        kvrpcpb::DsKvRawPutResponse resp;
        auto s = testRawPut(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        auto rng1 = range_server_->Find(1);
        ASSERT_TRUE(rng1 != nullptr);
        int b(0);
        static int aidx(0);
        static int pidx(0);
        do {
            pidx = ++aidx;
            std::static_pointer_cast<RangeSlave>(rng1->slave_range_)->Submit(1, pidx, aidx);

            if (b++ > 100) break;
            if (b%5 == 0) {
                FLOG_INFO("Submit...%d, size: %d", b,
                        std::static_pointer_cast<RangeSlave>(rng1->slave_range_)->trd_->size());
            }
        } while(true);

        // end test raw_put
    }
}
*/

