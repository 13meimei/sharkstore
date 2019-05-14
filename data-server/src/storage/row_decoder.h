_Pragma("once");

#include <map>
#include <string>

#include "base/status.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/exprpb.pb.h"
#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult&) = delete;
    RowResult& operator=(const RowResult&) = delete;

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
    RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const kvrpcpb::SelectRequest& req);

    RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const kvrpcpb::DeleteRequest& req);

    RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const txnpb::SelectRequest& req);

    ~RowDecoder();

    RowDecoder(const RowDecoder&) = delete;
    RowDecoder& operator=(const RowDecoder&) = delete;

    Status Decode(const std::string& key, const std::string& buf, RowResult& result);
    Status DecodeAndFilter(const std::string& key, const std::string& buf, RowResult& result, bool& match);

    std::string DebugString() const;

private:
    void addExprColumn(const exprpb::Expr& expr);
    Status decodePrimaryKeys(const std::string& key, RowResult& result);
    Status decodeFields(const std::string& buf, RowResult& result);

private:
    const std::vector<metapb::Column>& primary_keys_;
    std::map<uint64_t, exprpb::ColumnInfo> cols_; // 需要反解哪些列 （select的field list 和 where表达式中的）
    std::unique_ptr<exprpb::Expr> where_expr_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */