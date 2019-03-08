_Pragma("once");

#include "mem_store/mem_store.h"

#include "iterator.h"
#include "storage/db_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

using ColumnFamily = memstore::Store<std::string>;

class MemStore: public DbInterface {
public:
    MemStore() = default;
    ~MemStore() = default;

public:
    Status Open() override { return Status::OK(); }

    Status Get(const std::string& key, std::string* value) override;
    Status Get(void* column_family, const std::string& key, std::string* value) override;
    Status Put(const std::string& key, const std::string& value) override;

    std::unique_ptr<WriteBatchInterface> NewBatch() override;
    Status Write(WriteBatchInterface* batch) override;

    Status Delete(const std::string& key) override;
    Status Delete(void* column_family, const std::string& key) override;
    Status DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) override;

    void* DefaultColumnFamily() override;
    void* TxnCFHandle() override;

    IteratorInterface* NewIterator(const std::string& start, const std::string& limit) override;
    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                        std::unique_ptr<IteratorInterface>& txn_iter,
                        const std::string& start, const std::string& limit) override;
    void GetProperty(const std::string& k, std::string* v) override;

public:
    Status SetOptions(void* column_family,
                      const std::unordered_map<std::string, std::string>& new_options) override;
    Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) override;
    Status CompactRange(void* options, void* begin, void* end) override;
    Status Flush(void* fops) override;
    void PrintMetric() override;


private:
    memstore::Store<std::string> db_;
    ColumnFamily txn_cf_;
};


}}}
