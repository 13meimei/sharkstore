_Pragma("once");

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "base/status.h"
#include "storage/store.h"
#include "query_builder.h"

namespace sharkstore {
namespace test {
namespace helper {

class StoreTestFixture : public ::testing::Test {
public:
    explicit StoreTestFixture(std::unique_ptr<Table> t);

protected:
    void SetUp() override;
    void TearDown() override;

    Status testSelect(const std::function<void(SelectRequestBuilder&)>& build_func,
                      const std::vector<std::vector<std::string>>& expected_rows);

    Status testInsert(const std::vector<std::vector<std::string>> &rows);

    Status testDelete(const std::function<void(DeleteRequestBuilder&)>& build_func,
                      uint64_t expected_affected);

protected:
    std::unique_ptr<Table> table_;
    metapb::Range meta_;
    dataserver::storage::Store* store_ = nullptr;

private:
    std::string tmp_dir_;
    rocksdb::DB* db_ = nullptr;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
