//
// Created by zhangyongcheng on 19-2-28.
//
#include "where_expr.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

//get different data type value from Expr
FieldValue* CWhereExpr::GetExprVal(const std::string &inVal,
                              const metapb::Column &col)
{
    FieldValue *fv = nullptr;

    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(inVal.c_str(), NULL, 10);
                fv = new FieldValue(i);
            } else {
                uint64_t i = strtoull(inVal.c_str(), NULL, 10);
                fv = new FieldValue(i);
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(inVal.c_str(), NULL);
            fv = new FieldValue(d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            std::string *s = new std::string(inVal);
            fv = new FieldValue(s);
            break;
        }

        default:
            FLOG_ERROR("getExprVal() error, Invalid data_type: %d", col.data_type());
            return nullptr;
    }
//    FLOG_DEBUG("getExprVal()>>>const value: %s", fv->ToString().c_str());
    return fv;
}

Status CWhereExpr::ComputeExpr(const ::kvrpcpb::Expr& expr, const FieldValue *l,
                               const FieldValue *r, FieldValue **fv)
{
    std::string* str{nullptr};
    switch (expr.expr_type()) {
        case kvrpcpb::E_Plus:
            switch (l->Type()) {
                case FieldType::kInt:
                    *fv = new FieldValue(l->Int() + r->Int());
                    break;
                case FieldType::kUInt:
                    *fv = new FieldValue(l->UInt() + r->UInt());
                    break;
                case FieldType::kFloat:
                    *fv = new FieldValue(l->Float() + r->Float());
                    break;
                case FieldType::kBytes:
                    str = new std::string(l->Bytes());
                    str->append(r->Bytes());
                    *fv = new FieldValue(str);
                    break;
            }
            break;
        case kvrpcpb::E_Minus:
            switch (l->Type()) {
                case FieldType::kInt:
                    *fv = new FieldValue(l->Int() - r->Int());
                    break;
                case FieldType::kUInt:
                    *fv = new FieldValue(l->UInt() - r->UInt());
                    break;
                case FieldType::kFloat:
                    *fv = new FieldValue(l->Float() - r->Float());
                    break;
                case FieldType::kBytes:
                    str = new std::string(l->Bytes());
                    str->replace(str->find(r->Bytes()), r->Bytes().length(), "");
                    *fv = new FieldValue(str);
                    break;
            }
            break;
        case kvrpcpb::E_Mult:
            switch (l->Type()) {
                case FieldType::kInt:
                    *fv = new FieldValue(l->Int() * r->Int());
                    break;
                case FieldType::kUInt:
                    *fv = new FieldValue(l->UInt() * r->UInt());
                    break;
                case FieldType::kFloat:
                    *fv = new FieldValue(l->Float() * r->Float());
                    break;
                case FieldType::kBytes:
                    str = new std::string(l->Bytes());
                    str->append(r->Bytes());
                    *fv = new FieldValue(str);
                    break;
            }
            break;
        case kvrpcpb::E_Div:
            switch (l->Type()) {
                case FieldType::kInt:
                    if (r->Int() != 0) {
                        *fv = new FieldValue(l->Int() / r->Int());
                    }
                    break;
                case FieldType::kUInt:
                    if (r->UInt() != 0) {
                        *fv = new FieldValue(l->UInt() / r->UInt());
                    }
                    break;
                case FieldType::kFloat:
                    if (r->Float() != 0) {
                        *fv = new FieldValue(l->Float() / r->Float());
                    }
                    break;
                case FieldType::kBytes:
                    str = new std::string(l->Bytes());
                    str->replace(str->find(r->Bytes()), r->Bytes().length(), "");
                    *fv = new FieldValue(str);
                    break;
            }
            break;
        default:
            return Status(Status::kUnknown, "Unknown expr type", "");
    }

    return Status::OK();
}


bool CWhereExpr::CmpExpr(const FieldValue* l,
                         const FieldValue* r,
                         ::kvrpcpb::ExprType et) {
    switch (et) {
        case kvrpcpb::E_Equal:
            if (!fcompare(*l, *r, CompareOp::kEqual)) {
                return false;
            }
            break;
        case kvrpcpb::E_NotEqual: {
            bool ne = fcompare(*l, *r, CompareOp::kGreater) ||
                      fcompare(*l, *r, CompareOp::kLess);
            if (!ne) return false;
            break;
        }
        case kvrpcpb::E_Less:
            if (!fcompare(*l, *r, CompareOp::kLess)) {
                return false;
            }
            break;
        case kvrpcpb::E_LessOrEqual: {
            bool le = fcompare(*l, *r, CompareOp::kLess) ||
                      fcompare(*l, *r, CompareOp::kEqual);
            if (!le) return false;
            break;
        }
        case kvrpcpb::E_Larger:
            if (!fcompare(*l, *r, CompareOp::kGreater)) {
                return false;
            }
            break;
        case kvrpcpb::E_LargerOrEqual: {
            bool ge = fcompare(*l, *r, CompareOp::kGreater) ||
                      fcompare(*l, *r, CompareOp::kEqual);
            if (!ge) return false;
            break;
        }
        default:
            FLOG_ERROR("select unknown match type: %s",
                       kvrpcpb::ExprType_Name(et).c_str());
            return false;
    }
    return true;
}


}
}
}
