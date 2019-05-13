#include "util.h"

#include <chrono>

#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

using namespace std::chrono;

uint64_t calExpireAt(uint64_t ttl) {
    auto epoch = system_clock::now().time_since_epoch();
    return ttl + duration_cast<milliseconds>(epoch).count();
}

bool isExpired(uint64_t expired_at) {
    auto epoch = system_clock::now().time_since_epoch();
    auto now = duration_cast<milliseconds>(epoch).count();
    return static_cast<uint64_t>(now) > expired_at;
}

Status decodePK(const std::string& key, size_t& offset, const metapb::Column& col,
                std::unique_ptr<FieldValue>* field) {
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
                if (field != nullptr) field->reset(new FieldValue(i));
            } else {
                int64_t i = 0;
                if (!DecodeVarintAscending(key, offset, &i)) {
                    return Status(
                            Status::kCorruption,
                            std::string("decode row int pk failed at offset ") + std::to_string(offset),
                            EncodeToHexString(key));
                }
                if (field != nullptr) {
                    field->reset(new FieldValue(i));
                }
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
            if (field != nullptr) {
                field->reset(new FieldValue(d));
            }
            return Status::OK();
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string s;
            if (!DecodeBytesAscending(key, offset, &s)) {
                return Status(Status::kCorruption,
                              std::string("decode row string pk failed at offset ") +
                              std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(std::move(s)));
            }
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", col.name());
    }
}

Status decodeField(const std::string& buf, size_t& offset, const metapb::Column& col,
        std::unique_ptr<FieldValue>& field) {
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
                field.reset(new FieldValue(static_cast<uint64_t>(i)));
            } else {
                field.reset(new FieldValue(i));
            }
            return Status::OK();
        }

        case metapb::Float:
        case metapb::Double: {
            double d = 0;
            if (!DecodeFloatValue(buf, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row float value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(d));
            return Status::OK();
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string s;
            if (!DecodeBytesValue(buf, offset, &s)) {
                return Status(Status::kCorruption,
                              std::string("decode row string value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(std::move(s)));
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", col.name());
    }
}

Status parseThreshold(const std::string& thres, const metapb::Column& col, std::unique_ptr<FieldValue>& value) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(thres.c_str(), NULL, 10);
                value.reset(new FieldValue(i));
            } else {
                uint64_t i = strtoull(thres.c_str(), NULL, 10);
                value.reset(new FieldValue(i));
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(thres.c_str(), NULL);
            value.reset(new FieldValue(d));
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            value.reset(new FieldValue(thres));
            break;
        }

        default:
            return Status(Status::kNotSupported, "unknown match threshold col type", col.name());
    }
    return Status::OK();
}

Status matchField(FieldValue* field, const kvrpcpb::Match& filter, bool& matched) {
    if (field == nullptr) {
        matched = false;
        return Status::OK();
    }

    std::unique_ptr<FieldValue> threshold;
    auto s = parseThreshold(filter.threshold(), filter.column(), threshold);
    if (!s.ok()) {
        return s;
    }
    switch (filter.match_type()) {
        case kvrpcpb::Equal:
            matched = fcompare(*field, *threshold, CompareOp::kEqual);
            break;
        case kvrpcpb::NotEqual:
            matched = fcompare(*field, *threshold, CompareOp::kGreater) || fcompare(*field, *threshold, CompareOp::kLess);
            break;
        case kvrpcpb::Less:
            matched = fcompare(*field, *threshold, CompareOp::kLess);
            break;
        case kvrpcpb::LessOrEqual: {
            matched = fcompare(*field, *threshold, CompareOp::kLess) || fcompare(*field, *threshold, CompareOp::kEqual);
            break;
        }
        case kvrpcpb::Larger:
            matched = fcompare(*field, *threshold, CompareOp::kGreater);
            break;
        case kvrpcpb::LargerOrEqual:
            matched = fcompare(*field, *threshold, CompareOp::kGreater) || fcompare(*field, *threshold, CompareOp::kEqual);
            break;
        default:
            return Status(Status::kInvalidArgument, "math type", kvrpcpb::MatchType_Name(filter.match_type()));
    }
    return Status::OK();
}

Status matchRow(const RowResult& row, const std::vector<kvrpcpb::Match>& filters, bool& matched) {
    matched = true;
    for (const auto& filter: filters) {
        auto field = row.GetField(filter.column().id());
        auto s = matchField(field, filter, matched);
        if (!s.ok()) {
            return s;
        }
        if (!matched) {
            break;
        }
    }
    return Status::OK();
}

Status matchRow(const TxnRowValue& row, const std::vector<kvrpcpb::Match>& filters, bool& matched) {
    matched = true;
    for (const auto& filter: filters) {
        auto field = row.GetField(filter.column().id());
        auto s = matchField(field, filter, matched);
        if (!s.ok()) {
            return s;
        }
        if (!matched) {
            break;
        }
    }
    return Status::OK();
}

Status matchRow(const TxnRowValue& row, const std::shared_ptr<CWhereExpr> filter, bool& matched) {
    matched = filter->Filter(row);
    return Status::OK();
}

void fillColumnInfo(const metapb::Column& col, exprpb::ColumnInfo* info) {
    info->set_id(col.id());
    info->set_typ(col.data_type());
    info->set_unsigned_(col.unsigned_());
}

void makeColumnExpr(const metapb::Column& col, exprpb::Expr* expr) {
    ASSERT(expr != nullptr);
    expr->set_expr_type(exprpb::Column);
    auto column_info = expr->mutable_column();
    fillColumnInfo(col, column_info);
}

void makeConstValExpr(const metapb::Column& col, const std::string& value, exprpb::Expr* expr) {
    ASSERT(expr != nullptr);
    expr->set_value(value);
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: 
            expr->set_expr_type(col.unsigned_() ? exprpb::Const_UInt : exprpb::Const_Int);
            break;
        case metapb::Float:
        case metapb::Double: {
            expr->set_expr_type(exprpb::Const_Double);
            break;
        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp:
            expr->set_expr_type(exprpb::Const_Bytes);
            break;
        default:
            expr->set_expr_type(exprpb::Invalid);
    }
}

exprpb::ExprType toExprType(kvrpcpb::MatchType match_type) {
    switch (match_type) {
    case kvrpcpb::Equal:
        return exprpb::Equal;
    case kvrpcpb::NotEqual:
        return exprpb::Not_Equal;
    case kvrpcpb::Less:
        return exprpb::Less;
    case kvrpcpb::LessOrEqual:
        return exprpb::LessOrEqual;
    case kvrpcpb::Larger:
        return exprpb::Larger;
    case kvrpcpb::LargerOrEqual:
        return exprpb::LargerOrEqual;
    default:
        return exprpb::INVALID;
    }
}

std::unique_ptr<exprpb::Expr> convertToExpr(const MatchVector& matches) {
    if (matches.empty()) {
        return nullptr;
    }

    std::unique_ptr<exprpb::Expr> root(new expb::Expr);
    root->set_expr_type(exprpb::LogicAnd);
    for (const auto& m: matches) {
        auto cmp_expr = root->add_child();
        cmp_expr->set_expr_type(toExprType(m.match_type()));
        makeColumnExpr(m.column(), cmp_expr->add_child());
        makeConstValExpr(m.column(), m.threshold(), cmp_expr->add_child());
    }
    return root;
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */

