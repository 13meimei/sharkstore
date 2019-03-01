_Pragma("once");

#include <map>
#include <string>

#include "base/status.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/metapb.pb.h"
#include "where_expr.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

struct FieldValue;
struct FieldUpdate;
class CWhereExpr;

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult&) = delete;
    RowResult& operator=(const RowResult&) = delete;

    bool AddField(uint64_t col, FieldValue* fval);
    FieldValue* GetField(uint64_t col) const;

    void SetKey(const std::string& key) { key_ = key; }
    const std::string& Key() const { return key_; }

public:
    void SetValue(const std::string& value) { value_ = value; }
    const std::string& Value() const { return value_; }

    void AppendFieldValue(const FieldUpdate& fu) { field_value_.push_back(fu); }
    const std::vector<FieldUpdate>& FieldValueList() const { return field_value_; }

    void AddUpdateField(uint64_t id, kvrpcpb::Field* f) { update_field_.emplace(id, f); }
    const std::map<uint64_t, kvrpcpb::Field*>& UpdateFieldMap() const { return update_field_; }

    void AddUpdateFieldDelta(uint64_t id, FieldValue* v) { update_field_delta_.emplace(id, v); }
    const std::map<uint64_t, FieldValue*>& UpdateFieldDeltaMap() const { return update_field_delta_; }

    // 清空，方便迭代时重用
    void Reset();

private:
    std::string value_;
    std::vector<FieldUpdate> field_value_;
    std::map<uint64_t, kvrpcpb::Field*> update_field_;
    std::map<uint64_t, FieldValue*> update_field_delta_;

private:
    std::string key_;
    std::map<uint64_t, FieldValue*> fields_;

};

class RowDecoder {
public:
    RowDecoder(
        const std::vector<metapb::Column>& primary_keys,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches);

    RowDecoder(
            const std::vector<metapb::Column>& primary_keys,
            const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Field>& update_fields,
            const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches);

    RowDecoder(
        const std::vector<metapb::Column>& primary_keys,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::SelectField>& field_list,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches);

    RowDecoder(
            const std::vector<metapb::Column>& primary_keys,
            const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::SelectField>& field_list,
            const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches,
            const ::kvrpcpb::MatchExt& match_ext);

    ~RowDecoder();

    RowDecoder(const RowDecoder&) = delete;
    RowDecoder& operator=(const RowDecoder&) = delete;

    Status Decode(const std::string& key, const std::string& buf,
                  RowResult* result);
    Status Decode4Update(const std::string& key, const std::string& buf,
                         RowResult* result);

    Status DecodeAndFilter(const std::string& key, const std::string& buf,
                           RowResult* result, bool* matched);

    std::string DebugString() const;

    bool isExprValid() const {
        return (where_expr_ != nullptr);
    }
private:
    Status decodePrimaryKeys(const std::string& key, RowResult* result);

private:
    const std::vector<metapb::Column>& primary_keys_;
    std::map<uint64_t, metapb::Column> cols_;
    std::vector<kvrpcpb::Match> filters_;
    std::shared_ptr<CWhereExpr>  where_expr_{nullptr};

    std::map<uint64_t, kvrpcpb::Field> update_fields_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
