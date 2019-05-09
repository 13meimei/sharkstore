_Pragma("once");

#include <map>
#include <string>

#include "base/status.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/metapb.pb.h"
#include "where_expr.h"
#include "kv_fetcher.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class CWhereExpr;

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult&) = delete;
    RowResult& operator=(const RowResulRowResult = delete;

    const std::string& GetKey() const { return key_; }
    void SetKey(const std::string& key) { key_ = key; }

    uint64_t GetVersion() const { return version_; }
    void SetVersion(uint64_t ver) { version_ = ver; }

    bool AddField(uint64_t col, std::unique_ptr<FieldValue>& field);
    FieldValue* GetField(uint64_t col) const;

private:
    std::string key_;
    uint64_t version_ = 0;
    std::map<uint64_t, FieldValue*> fields_;
};

class RowDecoder {
public:
    explicit RowDecoder(const std::vector<metapb::Column>& primary_keys);
    ~RowDecoder();

    RowDecoder(const RowDecoder&) = delete;
    RowDecoder& operator=(const RowDecoder&) = delete;

    void Setup(const kvrpcpb::SelectRequest& req);
    void Setup(const kvrpcpb::DeleteRequest& req);
    void Setup(const txnpb::SelectRequest& req);

    Status Decode(const std::string& key, const std::string& buf, RowResult& result);
    Status DecodeAndFilter(const std::string& key, const std::string& buf, RowResult& result, bool& match);

    std::string DebugString() const;

private:
    Status decodePrimaryKeys(const std::string& key, RowResult& result);

private:
    const std::vector<metapb::Column>& primary_keys_;
    std::map<uint64_t, exprpb::ColumnInfo> cols_;
    std::unique_ptr<exprpb::Expr> where_filter_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
std::