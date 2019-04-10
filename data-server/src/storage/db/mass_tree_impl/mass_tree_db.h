_Pragma("once");

#include <memory>

#include "base/status.h"
#include "scaner.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// 包装MassTree的基本操作
class MassTreeDB {
public:
    MassTreeDB();
    ~MassTreeDB() = default;

    Status Put(const std::string& key, const std::string& value);
    Status Get(const std::string& key, std::string* value);
    Status Delete(const std::string& key);

    static void EpochIncr();

    template <typename F>
    int Scan(const std::string& begin, F& scanner);

    std::unique_ptr<Scaner> NewScaner(const std::string& start, const std::string& limit, size_t max_per_scan = 100);

private:
    thread_local static std::unique_ptr<threadinfo, ThreadInfoDeleter> thread_info_;

    TreeType* tree_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
