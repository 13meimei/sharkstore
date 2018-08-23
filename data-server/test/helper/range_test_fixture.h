_Pragma("once");

#include <gtest/gtest.h>
#include "range/range.h"
#include "range/context.h"
#include "mock/range_context_mock.h"

#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

// for test only
using namespace kvrpcpb;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;

class RangeTestFixture : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override;

protected:
    // version=0表示传range当前version
    void MakeHeader(RequestHeader *header, uint64_t version = 0);
    void SetLeader(uint64_t leader);

    Status Split();

    Status TestInsert(DsInsertRequest &req, DsInsertResponse *resp);
    Status TestSelect(DsSelectRequest& req, DsSelectResponse* resp);
    Status TestDelete(DsDeleteRequest& req, DsDeleteResponse* resp);

private:
    Status getResult(google::protobuf::Message *resp);

protected:
    std::unique_ptr<mock::RangeContextMock> context_;
    std::unique_ptr<Table> table_;
    std::shared_ptr<dataserver::range::Range> range_;
    uint64_t term_ = 0;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
