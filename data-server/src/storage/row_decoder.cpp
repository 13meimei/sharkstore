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
    setup(req);
}

RowDecoder::RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const kvrpcpb::DeleteRequest& req) : primary_keys_(primary_keys) {
    setup(req);
}

RowDecoder::RowDecoder(const std::vector<metapb::Column>& primary_keys,
        const txnpb::SelectRequest& req) : primary_keys_(primary_keys) {
    setup(req);
}

void RowDecoder::setup(const kvrpcpb::SelectRequest& req) {
    if (req.has_where_expr()) {
        where_expr_.reset(new exprpb::Expr(req.where_expr()));
    } else if (req.where_filters_size() > 0) {
        where_expr_ = convertToExpr(req.where_filters());
    }

    for (int i = 0; i < field_list.size(); i++) {
        const auto& field = field_list.Get(i);
        if (field.has_column()) {
            exprpb::ColumnInfo info;
            fillColumnInfo(field.column(), &info);
            cols_.emplace(field.column().id(), info);
        }
    }

    if (where_epxr_) {
        addExprColumn(*where_epxr_);
    }
}

void RowDecoder::setup(const kvrpcpb::DeleteRequest& req) {
    where_expr_ = convertToExpr(req.where_filters());
    if (where_epxr_) {
        addExprColumn(where_epxr_);
    }
}

void RowDecoder::setup(const txnpb::SelectRequest& req) {
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
        return Status::OK();
    } else if (!filters_.empty()) {
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
