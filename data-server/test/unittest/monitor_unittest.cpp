#include <gtest/gtest.h>

#include "monitor/system_status.h"
#include "monitor/statistics.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::monitor;

TEST(Monitor, Basic) {
    auto s = SystemStatus::New();
    uint64_t total = 0, available = 0;
    ASSERT_TRUE(s->GetFileSystemUsage(".", &total, &available));
    ASSERT_GT(total, 0);
    ASSERT_GT(available, 0);
    ASSERT_GE(total, available);

    ASSERT_TRUE(s->GetMemoryUsage(&total, &available));
    std::cout << "memory total: " << total << std::endl;
    std::cout << "memory available: " << available << std::endl;
}

TEST(Monitor, Statistics) {
    Statistics s;
    s.PushTime(HistogramType::kRaft, 123);
    std::cout << s.ToString() << std::endl;
    s.ToString();
}

} /* namespace  */
