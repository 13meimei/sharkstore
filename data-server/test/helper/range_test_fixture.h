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
    Status TestInsert(DsInsertRequest &req, DsInsertResponse *resp);
    Status TestSelect(DsSelectRequest& req, DsSelectResponse* resp);
    Status TestDelete(DsDeleteRequest& req, DsDeleteResponse* resp);

private:
    Status getResult(google::protobuf::Message *resp);

protected:
    std::unique_ptr<mock::RangeContextMock> context_;
    std::unique_ptr<Table> table_;
    std::unique_ptr<dataserver::range::Range> range_;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
