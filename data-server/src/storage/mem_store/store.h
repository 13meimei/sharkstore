_Pragma("once");

#include <mem_store/mem_store.h>

#include "iterator.h"
#include "storage/metric.h"
#include "range/split_policy.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/watchpb.pb.h"
#include "storage/field_value.h"
#include "storage/db_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class MemStore: public DbInterface {
public:
    MemStore() = default;
    ~MemStore() = default;

public:
    Status Get(const std::string& key, std::string* value);
    Status Get(void* column_family,
               const std::string& key, void* value);
    Status Put(const std::string& key, const std::string& value);
    Status Write(WriteBatchInterface* batch);
    Status Delete(const std::string& key);
    Status Delete(void* column_family, const std::string& key);
    Status DeleteRange(void* column_family,
                       const std::string& begin_key, const std::string& end_key);
    void* DefaultColumnFamily();
    void* TxnCFHandle();
    IteratorInterface* NewIterator(const std::string& start, const std::string& limit);
    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                        std::unique_ptr<IteratorInterface>& txn_iter,
                        const std::string& start = "", const std::string& limit = "");
    void GetProperty(const std::string& k, std::string* v);

public:
    Status SetOptions(void* column_family,
                      const std::unordered_map<std::string, std::string>& new_options);
    Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options);
    Status CompactRange(void* options, void* begin, void* end);
    Status Flush(void* fops);
    void PrintMetric();

    Status Insert(storage::Store* store,
                  const kvrpcpb::InsertRequest& req, uint64_t* affected);
    WriteBatchInterface* NewBatch();

private:
    memstore::Store<std::string> db_;
};


}}}
