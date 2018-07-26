#include <gtest/gtest.h>

#include <mutex>
#include <condition_variable>

#include "base/util.h"
#include "raft/src/impl/snapshot/task.h"
#include "raft/src/impl/snapshot/worker_pool.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using sharkstore::randomInt;
using namespace sharkstore::raft;
using namespace sharkstore::raft::impl;

void reporter(const SnapContext&, const SnapResult&) {}

class TestSnapTask : public SnapTask {
public:
    TestSnapTask(): SnapTask(randContext()) {
        SetReporter(reporter);
    }

    static SnapContext randContext() {
        SnapContext ctx;
        ctx.id = static_cast<uint64_t>(randomInt());
        ctx.term = static_cast<uint64_t>(randomInt());
        ctx.uuid = static_cast<uint64_t>(randomInt());
        ctx.from = static_cast<uint64_t>(randomInt());
        ctx.to = static_cast<uint64_t>(randomInt());
        return ctx;
    };

    void WaitRunning() {
        std::unique_lock<std::mutex> lock(mu_);
        while (state_ != State::kRunning) {
            cond_.wait(lock);
        }
    }

    void WaitFinish() {
        std::unique_lock<std::mutex> lock(mu_);
        while (state_ != State::kSuccess && state_ != State::kCanceled) {
            cond_.wait(lock);
        }
    }

    void Finish() {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = State::kSuccess;
        cond_.notify_all();
    }

    void Cancel() override {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = State::kCanceled;
        cond_.notify_all();
    }

    std::string Description() const override {
        return  std::string("Task[") + ID() + "]";
    }

private:
    void run(SnapResult *result) {
        std::unique_lock<std::mutex> lock(mu_);
        state_ = State::kRunning;
        cond_.notify_one();
        while (state_ != State::kSuccess && state_ != State::kCanceled) {
            cond_.wait(lock);
        }
        if (state_ == State::kSuccess) {
            result->blocks_count = static_cast<size_t>(randomInt());
            result->bytes_count = static_cast<size_t>(randomInt());
        }
    }

private:
    enum class State {
        kInitial,
        kRunning,
        kCanceled,
        kSuccess
    };

    State state_ = {State::kInitial};
    std::mutex mu_;
    std::condition_variable cond_;
};

TEST(Snapshot, WorkerPool) {
    size_t n = 5;
    SnapWorkerPool pool("test_snap", 5);

    std::vector<std::shared_ptr<TestSnapTask>> tasks;
    for (size_t i = 0; i < n; ++i) {
        auto t = std::make_shared<TestSnapTask>();
        auto ret = pool.Post(std::static_pointer_cast<SnapTask>(t));
        tasks.push_back(t);
        ASSERT_TRUE(ret);
    }
    // cloud not post any more
    auto t = std::make_shared<TestSnapTask>();
    auto ret = pool.Post(std::static_pointer_cast<SnapTask>(t));
    ASSERT_FALSE(ret);

    for (auto t: tasks) {
        t->WaitRunning();
    }
    ASSERT_EQ(pool.RunningsCount(), n);

    // finish one
    tasks.back()->Finish();
    tasks.back()->WaitFinish();
    tasks.pop_back();
    usleep(1000 * 100); // wait free worker
    ASSERT_EQ(pool.RunningsCount(), n - 1);

    // should be able to post new one
    t = std::make_shared<TestSnapTask>();
    ret = pool.Post(std::static_pointer_cast<SnapTask>(t));
    ASSERT_TRUE(ret);
    tasks.push_back(t);

    t->WaitRunning();
    ASSERT_EQ(pool.RunningsCount(), n);

    for (auto t: tasks) {
        t->Finish();
    }
    for (auto t: tasks) {
        t->WaitFinish();
    }
    usleep(1000 * 100); // wait free worker
    ASSERT_EQ(pool.RunningsCount(), 0);
}


}  // namespace

