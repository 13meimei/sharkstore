#include <gtest/gtest.h>
#include <fastcommon/shared_func.h>
#include <common/ds_config.h>
#include "helper/cpp_permission.h"

#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"
#include "proto/gen/schpb.pb.h"

#include "helper/table.h"
#include "helper/mock/raft_server_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace sharkstore::test::helper;
using namespace sharkstore::test::mock;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class DdlTest : public ::testing::Test {
protected:
    void SetUp() override {
        InitLog();

        strcpy(ds_config.engine_config.name, "rocksdb");
        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(NowMilliSeconds()).c_str());
        ds_config.range_config.recover_concurrency = 1;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
    }

    void TearDown() override {
        delete context_->range_server;
        delete context_->raft_server;
        delete context_;
        if (strlen(ds_config.rocksdb_config.path) > 0) {
            RemoveDirAll(ds_config.rocksdb_config.path);
        }
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};

metapb::Range *genRange() {
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

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

TEST_F(DdlTest, Ddl) {
    {
        // begin test create range
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange());
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
        // begin test create range (repeat)
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange());
        auto rpc = NewMockRPCRequest(req);

        range_server_->CreateRange(*rpc.first);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        schpb::CreateRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // end test create range
    }

    {
        // begin test create range (repeat but epoch stale)
        schpb::CreateRangeRequest req;
        auto meta = genRange();
        meta->mutable_range_epoch()->set_version(2);
        req.set_allocated_range(meta);

        auto rpc = NewMockRPCRequest(req);
        range_server_->CreateRange(*rpc.first);

        ASSERT_FALSE(range_server_->ranges_.empty());
        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        schpb::CreateRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_range());

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test delete range
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto rpc = NewMockRPCRequest(req);
        range_server_->DeleteRange(*rpc.first);

        ASSERT_TRUE(range_server_->ranges_.empty());
        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (not exist)
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto rpc = NewMockRPCRequest(req);
        range_server_->DeleteRange(*rpc.first);

        schpb::DeleteRangeResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        // end test delete range
    }
}
