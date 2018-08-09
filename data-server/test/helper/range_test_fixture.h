_Pragma("once");

#include <gtest/gtest.h>
#include "range/range.h"

#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

class RangeTestFixture : public ::testing::Test {
public:
    explicit RangeTestFixture(std::unique_ptr<Table> t);

protected:
    void SetUp() override;
    void TearDown() override;

private:
    void initContext();
    void destroyContext();

protected:
    std::unique_ptr<Table> table_;
    std::unique_ptr<dataserver::range::Range> range_;

private:
    std::string tmp_dir_;
    dataserver::server::ContextServer context_;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
