#include "row_decoder.h"

#include <algorithm>
#include <sstream>

#include "frame/sf_logger.h"
#include "common/ds_encoding.h"
#include "field_value.h"
#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

static Status parseThreshold(const std::string& thres, const metapb::Column& col,
                             std::unique_ptr<FieldValue>* value);

RowResult::RowResult() {}

RowResult::~RowResult() {
    Reset();
}

bool RowResult::AddField(uint64_t col, FieldValue* fval) {
    return fields_.emplace(col, fval).second;
}

FieldValue* RowResult::GetField(uint64_t col) const {
    auto it = fields_.find(col);
    if (it != fields_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void RowResult::Reset() {
    key_.clear();
    std::for_each(fields_.begin(), fields_.end(),
                  [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
    fields_.clear();

    value_.clear();
    field_value_.clear();

    update_field_.clear();
    std::for_each(update_field_delta_.begin(), update_field_delta_.end(),
                  [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
    update_field_delta_.clear();

}

RowDecoder::RowDecoder(
    const std::vector<metapb::Column>& primary_keys,
    const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches)
    : primary_keys_(primary_keys) {
    for (int i = 0; i < matches.size(); i++) {
        const auto& m = matches.Get(i);
        cols_.emplace(m.column().id(), m.column());
        filters_.push_back(m);
    }
}

RowDecoder::RowDecoder(
        const std::vector<metapb::Column>& primary_keys,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Field>& update_fields,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches)
        : RowDecoder{primary_keys, matches} {
    for (int i = 0; i < update_fields.size(); i++) {
        const auto& u = update_fields.Get(i);
        cols_.emplace(u.column().id(), u.column());
        update_fields_.emplace(u.column().id(), u);
    }
}

RowDecoder::RowDecoder(
    const std::vector<metapb::Column>& primary_keys,
    const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::SelectField>& field_list,
    const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches)
    : RowDecoder{primary_keys, matches} {
    for (int i = 0; i < field_list.size(); i++) {
        const auto& field = field_list.Get(i);
        if (field.has_column()) {
            cols_.emplace(field.column().id(), field.column());
        }
    }
}

RowDecoder::RowDecoder(
    const std::vector<metapb::Column>& primary_keys,
    const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::SelectField>& field_list,
    const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches,
    const ::kvrpcpb::MatchExt& match_ext)
        :RowDecoder(primary_keys, field_list, matches)
{
    where_expr_ = nullptr;
    if (match_ext.expr().child_size() > 0) {
        where_expr_ = std::make_shared<CWhereExpr>(match_ext);
    } else {
        FLOG_ERROR("origin filter with Match condition.");
    }
}

RowDecoder::~RowDecoder() {}

static Status decodePK(const std::string& key, size_t& offset, const metapb::Column& col,
                       FieldValue** value) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (col.unsigned_()) {
                uint64_t i = 0;
                if (!DecodeUvarintAscending(key, offset, &i)) {
                    return Status(
                            Status::kCorruption,
                            std::string("decode row unsigned int pk failed at offset ") + std::to_string(offset),
                            EncodeToHexString(key));
                }
                if (value != nullptr) *value = new FieldValue(i);
            } else {
                int64_t i = 0;
                if (!DecodeVarintAscending(key, offset, &i)) {
                    return Status(
                            Status::kCorruption,
                            std::string("decode row int pk failed at offset ") + std::to_string(offset),
                            EncodeToHexString(key));
                }
                if (value != nullptr) *value = new FieldValue(i);
            }
            return Status::OK();
        }

        case metapb::Float:
        case metapb::Double: {
            double d = 0;
            if (!DecodeFloatAscending(key, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row float pk failed at offset ") +
                              std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (value != nullptr) *value = new FieldValue(d);
            return Status::OK();
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string* s = new std::string();
            if (!DecodeBytesAscending(key, offset, s)) {
                delete s;
                return Status(Status::kCorruption,
                              std::string("decode row string pk failed at offset ") +
                              std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (value != nullptr) {
                *value = new FieldValue(s);
            } else {
                delete s;
            }
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", col.name());
    }
    return Status::OK();
}

Status RowDecoder::decodePrimaryKeys(const std::string& key, RowResult *result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient row key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!primary_keys_.empty());
    Status status;
    for (const auto& column: primary_keys_) {
        FieldValue* value = nullptr;
        auto it = cols_.find(column.id());
        if (it != cols_.end()) {
            status = decodePK(key, offset, column, &value);
        } else {
            status = decodePK(key, offset, column, nullptr);
        }
        if (!status.ok()) {
            delete value;
            return status;
        } else {
            if (value != nullptr) {
                if (!result->AddField(column.id(), value)) {
                    delete value;
                    return Status(Status::kDuplicate, "repeated field on column", column.name());
                }
            }
        }
    }
    return Status::OK();
}

static Status decodeField(const std::string& buf, size_t& offset, const metapb::Column& col,
                          FieldValue** value) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            int64_t i = 0;
            if (!DecodeIntValue(buf, offset, &i)) {
                return Status(
                    Status::kCorruption,
                    std::string("decode row int value failed at offset ") + std::to_string(offset),
                    EncodeToHexString(buf));
            }
            if (col.unsigned_()) {
                *value = new FieldValue(static_cast<uint64_t>(i));
            } else {
                *value = new FieldValue(i);
            }
            return Status::OK();
        }

        case metapb::Float:
        case metapb::Double: {
            double d = 0;
            if (!DecodeFloatValue(buf, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row float value failed at offset ") +
                                  std::to_string(offset),
                              EncodeToHexString(buf));
            }
            *value = new FieldValue(d);
            return Status::OK();
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string* s = new std::string();
            if (!DecodeBytesValue(buf, offset, s)) {
                delete s;
                return Status(Status::kCorruption,
                              std::string("decode row string value failed at offset ") +
                                  std::to_string(offset),
                              EncodeToHexString(buf));
            }
            *value = new FieldValue(s);
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", col.name());
    }
    return Status::OK();
}

Status RowDecoder::Decode4Update(const std::string& key, const std::string& buf, RowResult* result) {
    result->Reset();
    result->SetKey(key);
    result->SetValue(buf);

    // 解析主键列
    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) return s;

    // 解析非主键列
    uint32_t col_id = 0;
    EncodeType enc_type;
    bool ret = false;
    size_t tag_offset;
    for (size_t offset = 0; offset < buf.size();) {
        auto offset_bk = offset;

        // 解析列ID
        tag_offset = offset;
        ret = DecodeValueTag(buf, tag_offset, &col_id, &enc_type);
        if (!ret) {
            return Status(
                    Status::kCorruption,
                    std::string("decode row value tag failed at offset ") + std::to_string(offset),
                    EncodeToHexString(buf));
        }

        // 检查该列ID对应的列是否需要Decode
        auto it = cols_.find(col_id);
        if (it == cols_.end()) {
            ret = SkipValue(buf, offset);
            if (!ret) {
                return Status(
                        Status::kCorruption,
                        std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            // 记录所有非主键列的值在value中的偏移和长度
            FieldUpdate fu(col_id, offset_bk, offset - offset_bk);
            result->AppendFieldValue(fu);
            // 记录需要update列
            auto it_u = update_fields_.find(col_id);
            if (it_u != update_fields_.end()) {
                auto& f = it_u->second;

                result->AddUpdateField(col_id, &f);

                // 解析kvrpcfield为fieldvalue
                std::unique_ptr<FieldValue> cf = nullptr;
                auto s = parseThreshold(f.value(), f.column(), &cf);
                if (!s.ok()) {
                    FLOG_ERROR("parse update field value failed: %s", s.ToString().c_str());
                    return Status(Status::kUnknown, std::string("parse update field value failed:1 " + s.ToString()), "");
                }
                result->AddUpdateFieldDelta(col_id, cf.release());
            }

            continue;
        }

        // 解码列值
        FieldValue* value = nullptr;
        auto status = decodeField(buf, offset, it->second, &value);
        if (!status.ok()) {
            delete value;
            return status;
        } else {
            if (!result->AddField(it->first, value)) {
                delete value;
                return Status(Status::kDuplicate, "repeated field on column", it->second.name());
            }
        }

        // 记录所有非主键列的值在value中的偏移和长度
        FieldUpdate fu(col_id, offset_bk, offset - offset_bk);
        result->AppendFieldValue(fu);
        // 记录需要update列
        auto it_u = update_fields_.find(col_id);
        if (it_u != update_fields_.end()) {
            auto& f = it_u->second;

            result->AddUpdateField(col_id, &f);

            // 解析kvrpcfield为fieldvalue
            std::unique_ptr<FieldValue> cf = nullptr;
            auto s = parseThreshold(f.value(), f.column(), &cf);
            if (!s.ok()) {
                FLOG_ERROR("parse update field value failed: %s", s.ToString().c_str());
                return Status(Status::kUnknown, std::string("parse update field value failed:2 " + s.ToString()), "");
            }
            result->AddUpdateFieldDelta(col_id, cf.release());
        }
    }
    return Status::OK();
}

Status RowDecoder::Decode(const std::string& key, const std::string& buf, RowResult* result) {
    assert(result != nullptr);
    if (update_fields_.size() != 0) {
        return Decode4Update(key, buf, result);
    }

    result->Reset();
    result->SetKey(key);

    // 解析主键列
    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) return s;

    // 解析非主键列
    uint32_t col_id = 0;
    EncodeType enc_type;
    bool ret = false;
    size_t tag_offset;
    for (size_t offset = 0; offset < buf.size();) {
        // 解析列ID
        tag_offset = offset;
        ret = DecodeValueTag(buf, tag_offset, &col_id, &enc_type);
        if (!ret) {
            return Status(
                Status::kCorruption,
                std::string("decode row value tag failed at offset ") + std::to_string(offset),
                EncodeToHexString(buf));
        }

        // 检查该列ID对应的列是否需要Decode
        auto it = cols_.find(col_id);
        if (it == cols_.end()) {
            ret = SkipValue(buf, offset);
            if (!ret) {
                return Status(
                    Status::kCorruption,
                    std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                    EncodeToHexString(buf));
            }
            continue;
        }

        // 解码列值
        FieldValue* value = nullptr;
        auto status = decodeField(buf, offset, it->second, &value);
        if (!status.ok()) {
            delete value;
            return status;
        } else {
            if (!result->AddField(it->first, value)) {
                delete value;
                FLOG_DEBUG("add field id: %lu", it->second.id());
                return Status(Status::kDuplicate, "repeated field on column", it->second.name());
            }
        }
    }
    return Status::OK();
}

static Status parseThreshold(const std::string& thres, const metapb::Column& col,
                             std::unique_ptr<FieldValue>* value) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(thres.c_str(), NULL, 10);
                value->reset(new FieldValue(i));
            } else {
                uint64_t i = strtoull(thres.c_str(), NULL, 10);
                value->reset(new FieldValue(i));
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(thres.c_str(), NULL);
            value->reset(new FieldValue(d));
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string* s = new std::string(thres);
            value->reset(new FieldValue(s));
            break;
        }

        default:
            return Status(Status::kNotSupported, "unknown match threshold col type", col.name());
    }
    return Status::OK();
}

static bool filter(const RowResult& result, const std::vector<kvrpcpb::Match>& filters) {
    for (auto it = filters.cbegin(); it != filters.cend(); ++it) {
        const kvrpcpb::Match& m = *it;
        auto f = result.GetField(m.column().id());
        if (nullptr == f) {
            return false;
        }
        std::unique_ptr<FieldValue> cf = nullptr;
        auto s = parseThreshold(m.threshold(), m.column(), &cf);
        if (!s.ok()) {
            FLOG_ERROR("select parse threshold failed: %s", s.ToString().c_str());
            return false;
        }
        assert(cf != nullptr);
        switch (m.match_type()) {
            case kvrpcpb::Equal:
                if (!fcompare(*f, *cf, CompareOp::kEqual)) return false;
                break;
            case kvrpcpb::NotEqual: {
                bool not_equal =
                    fcompare(*f, *cf, CompareOp::kGreater) || fcompare(*cf, *f, CompareOp::kLess);
                if (!not_equal) return false;
                break;
            }
            case kvrpcpb::Less:
                if (!fcompare(*f, *cf, CompareOp::kLess)) return false;
                break;
            case kvrpcpb::LessOrEqual: {
                bool le =
                    fcompare(*f, *cf, CompareOp::kLess) || fcompare(*cf, *f, CompareOp::kEqual);
                if (!le) return false;
                break;
            }
            case kvrpcpb::Larger:
                if (!fcompare(*f, *cf, CompareOp::kGreater)) return false;
                break;
            case kvrpcpb::LargerOrEqual: {
                bool ge =
                    fcompare(*f, *cf, CompareOp::kGreater) || fcompare(*cf, *f, CompareOp::kEqual);
                if (!ge) return false;
                break;
            }
            default:
                FLOG_ERROR("select unknown match type: %s", kvrpcpb::MatchType_Name(m.match_type()).c_str());
                return false;
        }
    }
    return true;
}

static bool filter_ext(const RowResult& result, const std::shared_ptr<CWhereExpr> where) {
    return where->Filter(result);
}

Status RowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult* result, bool* matched) {
    assert(result != nullptr);

    auto s = Decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }

    *matched = true;
    if (isExprValid()) {
        *matched = filter_ext(*result, where_expr_);
    } else if (!filters_.empty()) {
        *matched = filter(*result, filters_);
    }

    return Status::OK();
}

std::string RowDecoder::DebugString() const {
    std::ostringstream ss;
    ss << "filters: [";
    for (const auto& f : filters_) {
        ss << f.ShortDebugString();
    }
    ss << "]";
    return ss.str();
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
