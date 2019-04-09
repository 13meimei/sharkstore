_Pragma("once");

#include "scaner.h"
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// 包装MassTree的基本操作
class Scaner;
class MassTreeDB {
public:
    MassTreeDB();
    ~MassTreeDB() = default;

    Status Put(const std::string& key, const std::string& value);
    Status Get(const std::string& key, std::string* value);
    Status Delete(const std::string& key);

    void EpochIncr();

    std::unique_ptr<Scaner> NewScaner(const std::string& start, const std::string& limit, size_t max_per_scan = 100);

private:
    thread_local static std::unique_ptr<threadinfo, ThreadInfoDeleter> thread_info_;

    TreeType* tree_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
