#include "row_decoder.h"

#include <algorithm>
#include <sstream>

#include "frame/sf_logger.h"
#include "common/ds_encoding.h"
#include "field_value.h"
#include "store.h"
#include "util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

RowResult::RowResult() {}

RowResult::~RowResult() {
    Reset();
}

bool RowResult::AddField(uint64_t col, std::unique_ptr<FieldValue>& field) {
    auto ret = fields_.emplace(col, field.get()).second;
    if (ret) {
        auto p = field.release();
        (void)p;
    }
    return ret;
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

RowDecoder::~RowDecoder() {}


Status RowDecoder::decodePrimaryKeys(const std::string& key, RowResult *result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient row key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!primary_keys_.empty());
    Status status;
    for (const auto& column: primary_keys_) {
        std::unique_ptr<FieldValue> value;
        auto it = cols_.find(column.id());
        if (it != cols_.end()) {
            status = decodePK(key, offset, column, &value);
        } else {
            status = decodePK(key, offset, column, nullptr);
        }
        if (!status.ok()) {
            return status;
        }
        if (value != nullptr) {
            if (!result->AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", column.name());
            }
        }
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
                std::unique_ptr<FieldValue> cf;
                s = parseThreshold(f.value(), f.column(), cf);
                if (!s.ok()) {
                    FLOG_ERROR("parse update field value failed: %s", s.ToString().c_str());
                    return Status(Status::kUnknown, std::string("parse update field value failed:1 " + s.ToString()), "");
                }
                // TODO: fix release
                result->AddUpdateFieldDelta(col_id, cf.release());
            }

            continue;
        }

        // 解码列值
        std::unique_ptr<FieldValue> value;
        auto status = decodeField(buf, offset, it->second, value);
        if (!status.ok()) {
            return status;
        }
        if (!result->AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", it->second.name());
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
            std::unique_ptr<FieldValue> cf;
            s = parseThreshold(f.value(), f.column(), cf);
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
        std::unique_ptr<FieldValue> value;
        auto status = decodeField(buf, offset, it->second, value);
        if (!status.ok()) {
            return status;
        }
        if (!result->AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", it->second.name());
        }
    }
    return Status::OK();
}


Status RowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult* result, bool* matched) {
    assert(result != nullptr);

    auto s = Decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }

    *matched = true;
    if (!filters_.empty()) {
        return matchRow(*result, filters_, *matched);
    } else {
        return Status::OK();
    }
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
