#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "test_util.h"
#include "base/util.h"
#include "common/ds_proto.h"

#include "raft/snapshot.h"
#include "raft/statemachine.h"
#include "raft/src/impl/snapshot/apply_task.h"
#include "raft/src/impl/snapshot/send_task.h"
#include "raft/src/impl/transport/inprocess_transport.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;
using namespace sharkstore::raft;
using namespace sharkstore::raft::impl;
using sharkstore::raft::impl::testutil::randSnapContext;

class TestSnapshot : public Snapshot {
public:
    TestSnapshot() :
        applied_(randomInt()),
        count_(100 + randomInt() % 100),
        context_(randomString(100 + randomInt() % 100))
    {
    }

    uint64_t ApplyIndex() override { return applied_; }
    uint64_t GetCount() const { return count_; }
    const std::string& GetContext() const { return context_; }

    Status Next(std::string* data, bool* over) override {
        if (index_ > count_) {
            *over = true;
        } else {
            *over = false;
            *data = std::to_string(index_++);
        }
        return Status::OK();
    };

    Status Context(std::string* context) override {
        *context = context_;
        return Status::OK();
    }

    void Close() override {}

private:
    const uint64_t applied_ = 0;
    const uint64_t count_ = 0;
    const std::string context_;
    uint64_t index_ = 1;
};

class TestStateMachine : public raft::StateMachine {
public:
    explicit TestStateMachine(std::shared_ptr<TestSnapshot> expected) :
        expected_(expected) {
    }

    Status Apply(const std::string& cmd, uint64_t index) override  {
        return Status(Status::kNotSupported);
    }
    Status ApplyMemberChange(const ConfChange& cc, uint64_t index) override {
        return Status(Status::kNotSupported);
    }
    void OnReplicateError(const std::string& cmd, const Status& status) override  {}
    void OnLeaderChange(uint64_t leader, uint64_t term) override {}
    std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }

    Status ApplySnapshotStart(const std::string& context) {
        if (context != expected_->GetContext()) {
            return Status(Status::kInvalid, "snapshot context",
                          context + " != " + expected_->GetContext());
        }
        return Status::OK();
    }

    Status ApplySnapshotData(const std::vector<std::string>& datas) {
        for (const auto& data: datas) {
            auto i = std::strtoul(data.c_str(), NULL, 10);
            if (i != pre_num_ + 1) {
                return Status(Status::kInvalid, "snapshot data",
                              std::to_string(pre_num_) + "+1 != " + data);
            }
            ++pre_num_;
        }
        return Status::OK();
    }

    Status ApplySnapshotFinish(uint64_t index) override {
        Status s;
        if (pre_num_ != expected_->GetCount()) {
            s = Status(Status::kInvalid, "snapshot count",
                          std::to_string(pre_num_) + " != "  + std::to_string(expected_->GetCount()));
        } else if (index != expected_->ApplyIndex()) {
            s = Status(Status::kInvalid, "snapshot index",
                          std::to_string(index) + " != "  + std::to_string(expected_->ApplyIndex()));

        }
        std::lock_guard<std::mutex> lock(mu_);
        finished_ = true;
        cond_.notify_one();
        return s;
    }

    void WaitFinished() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!finished_) {
            cond_.wait(lock);
        }
    }

private:
    std::shared_ptr<TestSnapshot> expected_;
    uint64_t pre_num_ = 0;
    std::mutex mu_;
    std::condition_variable cond_;
    bool finished_ = false;
};


TEST(Snapshot, SendAndApply) {
    const uint64_t kSendNodeID = 1;
    const uint64_t kApplyNodeID = 2;
    const uint64_t kSnapTerm = randomInt();
    const uint64_t kSnapUUID = randomInt();
    const uint64_t kRaftID = randomInt();

    auto snap = std::make_shared<TestSnapshot>();

    // prepare apply task
    SnapContext apply_ctx;
    apply_ctx.to = kApplyNodeID;
    apply_ctx.from = kSendNodeID;
    apply_ctx.uuid = kSnapUUID;
    apply_ctx.id = kRaftID;
    apply_ctx.term = kSnapTerm;

    auto sm = std::make_shared<TestStateMachine>(snap);
    auto receiver = std::make_shared<ApplySnapTask>(apply_ctx, sm);
    receiver->SetOptions(ApplySnapTask::Options());

    receiver->SetReporter([&apply_ctx](const SnapContext& ctx, const SnapResult& result) {
        auto s = testutil::Equal(ctx, apply_ctx);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(result.status.ok()) << result.status.ToString();
        std::cout << "apply bytes: " << result.bytes_count <<
                  ", blocks: " << result.blocks_count << std::endl;
        ASSERT_GT(result.bytes_count, 0);
        ASSERT_GT(result.blocks_count, 0);
    });

    auto apply_trans = new transport::InProcessTransport(kApplyNodeID);
    auto s = apply_trans->Start("", 1234, [=](MessagePtr& msg) {
        auto s = receiver->RecvData(msg);
        ASSERT_TRUE(s.ok()) << "Recv error: " << s.ToString();
    });
    ASSERT_TRUE(s.ok()) << s.ToString();
    receiver->SetTransport(apply_trans);


    // prepare send task
    SnapContext send_ctx;
    send_ctx.to = kApplyNodeID;
    send_ctx.from = kSendNodeID;
    send_ctx.id = kRaftID;
    send_ctx.term = kSnapTerm;
    send_ctx.uuid = kSnapUUID;

    pb::SnapshotMeta meta;
    meta.set_index(snap->ApplyIndex());
    meta.set_term(kSnapTerm);
    meta.set_context(snap->GetContext());

    auto sender = std::make_shared<SendSnapTask>(send_ctx, std::move(meta), snap);
    SendSnapTask::Options sops;
    sops.max_size_per_msg = 10;
    sender->SetOptions(sops);

    auto send_trans = new transport::InProcessTransport(kSendNodeID);
    s = send_trans->Start("", 1234, [=](MessagePtr& msg) {
        auto s = sender->RecvAck(msg);
        ASSERT_TRUE(s.ok()) << "Recv error: " << s.ToString();
    });
    sender->SetTransport(send_trans);

    sender->SetReporter([&send_ctx](const SnapContext& ctx, const SnapResult& result) {
        auto s = testutil::Equal(ctx, send_ctx);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(result.status.ok()) << result.status.ToString();
        ASSERT_GT(result.bytes_count, 0);
        ASSERT_GT(result.blocks_count, 0);
        std::cout << "send bytes: " << result.bytes_count <<
                  ", blocks: " << result.blocks_count << std::endl;
    });

    // start to send
    std::thread send_thr([=] { sender->Run(); });
    send_thr.detach();

    // start to recv and apply
    std::thread apply_thr([=] { receiver->Run(); });
    apply_thr.detach();

    sm->WaitFinished();

    delete apply_trans;
    delete send_trans;
}

}  // namespace
