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
    std::for_each(fields_.begin(), fields_.end(),
                  [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
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

void RowResult::Encode(const txnpb::SelectRequest& req, txnpb::RowValue* to) {
    std::string buf;
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            auto fv = GetField(field.column().id());
            EncodeFieldValue(&buf, fv);
        }
    }
    to->set_fields(buf);
    to->set_version(version_);
}

RowDecoder::RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const kvrpcpb::SelectRequest& req) : primary_keys_(primary_keys) {
    // where过滤条件
    if (req.has_where_expr()) {
        where_expr_.reset(new exprpb::Expr(req.where_expr()));
    } else if (req.where_filters_size() > 0) {
        where_expr_ = convertToExpr(req.where_filters());
    }

    // 根据select field list选择需要解码的列
    for (int i = 0; i < field_list.size(); i++) {
        const auto& field = field_list.Get(i);
        if (field.has_column()) {
            exprpb::ColumnInfo info;
            fillColumnInfo(field.column(), &info);
            cols_.emplace(field.column().id(), std::move(info));
        }
    }

    // where条件如果有列表达式，也需要解码
    if (where_epxr_) {
        addExprColumn(*where_epxr_);
    }
}

RowDecoder::RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const kvrpcpb::DeleteRequest& req) : primary_keys_(primary_keys) {
    where_expr_ = convertToExpr(req.where_filters());
    if (where_epxr_) {
        addExprColumn(where_epxr_);
    }
}

RowDecoder::RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const txnpb::SelectRequest& req) : primary_keys_(primary_keys) {
    // where过滤条件
    if (req.has_where_expr()) {
        where_expr_.reset(new exprpb::Expr(req.where_expr()));
    } else if (req.where_filters_size() > 0) {
        where_expr_ = convertToExpr(req.where_filters());
    }
    // 根据select field list选择需要解码的列
    for (int i = 0; i < field_list.size(); i++) {
        const auto& field = field_list.Get(i);
        if (field.has_column()) {
            exprpb::ColumnInfo info;
            fillColumnInfo(field.column(), &info);
            cols_.emplace(field.column().id(), std::move(info));
        }
    }
    if (where_epxr_) {
        addExprColumn(where_epxr_);
    }
}

void RowDecoder::addExprColumn(const exprpb::Expr& expr) {
    if (expr.expr_type() == exprpb::Column) {
        cols_.emplace(expr.column().id(), expr.column());
    }
    for (const auto& child: expr.child()) {
        addExprColumn(child);
    }
}

RowDecoder::~RowDecoder() = default;

Status RowDecoder::decodePrimaryKeys(const std::string& key, RowResult& result) {
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
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", column.name());
            }
        }
    }
    return Status::OK();
}

Status RowDecoder::decodeFields(const std::string& buf, RowResult& result) {
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t tag_offset = 0;
    for (size_t offset = 0; offset < buf.size();) {
        // 解析列ID
        tag_offset = offset;
        if (!DecodeValueTag(buf, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode row value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        // 解码version列
        if (col_id == kVersionColumnID) {
            int64_t version = 0;
            if (DecodeIntValue(buf, offset, &version)) {
                result.SetVersion(static_cast<uint64_t>(version));
            } else {
                return Status(Status::kCorruption,
                              std::string("decode version value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        }
        // 检查该列ID对应的列是否需要Decode
        auto it = cols_.find(col_id);
        if (it == cols_.end()) {
            if (!SkipValue(buf, offset)) {
                return Status(Status::kCorruption,
                              std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        }
        // 解码列值
        std::unique_ptr<FieldValue> value;
        auto s = decodeField(buf, offset, it->second, value);
        if (!s.ok()) {
            return s;
        }
        if (!result.AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", it->second.name());
        }
    }
    return Status::OK();
}

Status RowDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {
    // 解析key中的主键列
    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) {
        return s;
    }
    // 解析value中的非主键列
    s = decodeFields(buf, row);
    if (!s.ok()) {
        return s;
    }
    result->SetKey(key);
    return Status::OK();
}

Status RowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult& result, bool& matched) {
    auto s = Decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }

    matched = true;
    if (where_expr_) {
        return filterExpr(result, *where_expr_, matched);
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
