_Pragma("once");

#include <memory>
#include <vector>

#include "db_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// 内存中DB的WriteBatch实现，缓存每个操作，Write时整体写入
// TODO: limit max batch size
class MemDBWriteBatch : public WriteBatchInterface {
public:
    MemDBWriteBatch() = default;
    ~MemDBWriteBatch() = default;

    Status Put(const std::string &key, const std::string &value) override;
    Status Put(void* column_family, const std::string& key, const std::string& value) override;
    Status Delete(const std::string &key) override;
    Status Delete(void* column_family, const std::string& key) override;

    Status WriteTo(DbInterface* db);

private:
    enum class EntryType {
        kPut,
        kDelete,
    };

    struct BatchEntry {
        void *cf = nullptr;
        EntryType type = EntryType::kPut;
        std::string key;
        std::string value;
    };

private:
    std::vector<std::unique_ptr<BatchEntry>> entries_;
};


} // namespace storage
} // namespace dataserver
} // namespace sharkstore
