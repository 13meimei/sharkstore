_Pragma("once");

#include <mutex>

#include "metric.h"
#include "range/split_policy.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/txn.pb.h"
#include "field_value.h"
#include "db/db_interface.h"
#include "db/iterator_interface.h"
#include "raft/snapshot.h"
#include "txn_iterator.h"

// test fixture forward declare for friend class
namespace sharkstore { namespace test { namespace helper { class StoreTestFixture; }}}

namespace sharkstore {
namespace dataserver {
namespace storage {

using TxnErrorPtr = std::unique_ptr<txnpb::TxnError>;

// 行前缀长度: 1字节特殊标记+8字节table id
static const size_t kRowPrefixLength = 9;
static const unsigned char kStoreKVPrefixByte = '\x01';

static const size_t kDefaultMaxSelectLimit = 10000;

static const uint32_t kVersionColumnID = std::numeric_limits<uint32_t>::max();

class DbInterface;
class Store {
public:
    Store(const metapb::Range& meta, DbInterface* db);
    ~Store();

    Store(const Store&) = delete;
//    Store& operator=(const Store&) = delete;

    void SetEndKey(std::string end_key);
    std::string GetEndKey() const;

    const std::vector<metapb::Column>& GetPrimaryKeys() const { return primary_keys_; }

    void ResetMetric() { metric_.Reset(); }
    void CollectMetric(MetricStat* stat) { metric_.Collect(stat); }

    // 统计存储实际大小，并且根据split_size返回中间key
    Status StatSize(uint64_t split_size, range::SplitKeyMode mode,
                    uint64_t *real_size, std::string *split_key);
    // 从rocksdb中删除当前range的数据
    Status Truncate();

    Status Get(const std::string& key, std::string* value);
    Status Put(const std::string& key, const std::string& value);
    Status Delete(const std::string& key);

    Status Insert(const kvrpcpb::InsertRequest& req, uint64_t* affected);
    Status Update(const kvrpcpb::UpdateRequest& req, uint64_t* affected, uint64_t* update_bytes);
    Status Select(const kvrpcpb::SelectRequest& req,
                  kvrpcpb::SelectResponse* resp);
    Status DeleteRows(const kvrpcpb::DeleteRequest& req, uint64_t* affected);

    // watch funcs
    Status WatchPut(const watchpb::KvWatchPutRequest& req, int64_t version);
    Status WatchDelete(const watchpb::KvWatchDeleteRequest& req);
    Status WatchGet(const watchpb::DsKvWatchGetMultiRequest& req, watchpb::DsKvWatchGetMultiResponse *resp);

    Status GetTxnValue(const std::string& key, std::string& db_value);
    Status GetTxnValue(const std::string& key, txnpb::TxnValue* value);
    std::unique_ptr<TxnIterator> NewTxnIterator(const std::string& start = "", const std::string& limit = "");

    void TxnPrepare(const txnpb::PrepareRequest& req, uint64_t version, txnpb::PrepareResponse* resp);
    uint64_t TxnDecide(const txnpb::DecideRequest& req, txnpb::DecideResponse* resp);
    void TxnClearup(const txnpb::ClearupRequest& req, txnpb::ClearupResponse* resp);
    void TxnGetLockInfo(const txnpb::GetLockInfoRequest& req, txnpb::GetLockInfoResponse* resp);
    Status TxnSelect(const txnpb::SelectRequest& req, txnpb::SelectResponse* resp);
    Status TxnScan(const txnpb::ScanRequest& req, txnpb::ScanResponse* resp);

public:
    IteratorInterface* NewIterator(const ::kvrpcpb::Scope& scope);
    IteratorInterface* NewIterator(std::string start = std::string(),
                          std::string limit = std::string());
    Status BatchDelete(const std::vector<std::string>& keys);
    bool KeyExists(const std::string& key);
    Status BatchSet(
        const std::vector<std::pair<std::string, std::string>>& keyValues);
    Status RangeDelete(const std::string& start, const std::string& limit);

    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter, std::unique_ptr<IteratorInterface>& txn_iter,
            const std::string& start = "", const std::string& limit = "");

    Status GetSnapshot(uint64_t apply_index, std::string&& context,
            std::shared_ptr<raft::Snapshot>* snapshot);
    Status ApplySnapshot(const std::vector<std::string>& datas);

public:
    void addMetricRead(uint64_t keys, uint64_t bytes);
    void addMetricWrite(uint64_t keys, uint64_t bytes);

private:
    friend class RowFetcher;
    friend class ::sharkstore::test::helper::StoreTestFixture;

    Status selectSimple(const kvrpcpb::SelectRequest& req,
                        kvrpcpb::SelectResponse* resp);
    Status selectAggre(const kvrpcpb::SelectRequest& req,
                       kvrpcpb::SelectResponse* resp);

    std::string encodeWatchKey(const watchpb::WatchKeyValue& kv) const;
    std::string encodeWatchValue(const watchpb::WatchKeyValue& kv, int64_t version) const;
    bool decodeWatchKey(const std::string& key, watchpb::WatchKeyValue *kv) const;
    bool decodeWatchValue(const std::string& value, watchpb::WatchKeyValue *kv) const;

    Status writeTxnValue(const txnpb::TxnValue& value, WriteBatchInterface* batch);
    TxnErrorPtr checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag);
    Status getKeyVersion(const std::string& key, uint64_t *version);
    TxnErrorPtr checkUniqueAndVersion(const txnpb::TxnIntent& intent);
    TxnErrorPtr prepareIntent(const txnpb::PrepareRequest& req, const txnpb::TxnIntent& intent,
            uint64_t version, WriteBatchInterface* batch);

    Status commitIntent(const txnpb::TxnIntent& intent, uint64_t version,
            uint64_t &bytes_written, WriteBatchInterface* batch);
    TxnErrorPtr decidePrimary(const txnpb::DecideRequest& req, uint64_t& bytes_written,
            WriteBatchInterface* batch, txnpb::DecideResponse* resp);
    TxnErrorPtr decideSecondary(const txnpb::DecideRequest& req, const std::string& key,
            uint64_t& bytes_written, WriteBatchInterface* batch);

    Status parseSplitKey(const std::string& key, range::SplitKeyMode mode, std::string *split_key);

private:
    const uint64_t table_id_ = 0;
    const uint64_t range_id_ = 0;
    const std::string start_key_;

    std::string end_key_;
    mutable std::mutex key_lock_;

    DbInterface* db_;

    std::vector<metapb::Column> primary_keys_;

    Metric metric_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
