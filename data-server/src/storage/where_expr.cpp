//
// Created by zhangyongcheng on 19-2-28.
//
#include "where_expr.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

//check parameter
static bool check(const ::kvrpcpb::Expr &e) {
    switch (e.expr_type()) {
        case ::kvrpcpb::E_LogicAnd:
        case ::kvrpcpb::E_LogicOr:
        case ::kvrpcpb::E_Equal:
        case ::kvrpcpb::E_NotEqual:
        case ::kvrpcpb::E_Less:
        case ::kvrpcpb::E_LessOrEqual:
        case ::kvrpcpb::E_Larger:
        case ::kvrpcpb::E_LargerOrEqual:
        case ::kvrpcpb::E_Plus:
        case ::kvrpcpb::E_Minus:
        case ::kvrpcpb::E_Mult:
        case ::kvrpcpb::E_Div:
            if (e.child_size() != 2) {
                FLOG_WARN("check() expr_type: %d child_size: %d",
                           e.expr_type(), e.child_size());
                return false;
            }
            return true;
        case ::kvrpcpb::E_LogicNot:
            if (e.child_size() != 1) {
                return false;
            }
            return true;
        case ::kvrpcpb::E_ExprCol: //leaf node
        case ::kvrpcpb::E_ExprConst:
            if (e.child_size() != 0) {
                return false;
            }
            return true;
        default:
            FLOG_ERROR("check() error, INVALID expr_type");
            return false;
    }
}

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

FieldValue* CWhereExpr::GetValFromExpr(
        const RowResult &result,
        const ::kvrpcpb::Expr &expr)
{
    FieldValue *fv = nullptr;
    std::shared_ptr<FieldValue> l = nullptr;
    std::shared_ptr<FieldValue> r = nullptr;

    switch (expr.expr_type()) {
        case ::kvrpcpb::E_ExprCol:
            fv = result.GetField(expr.column().id());
            break;
        case ::kvrpcpb::E_ExprConst:
            fv = GetExprVal(expr.value(), expr.column());
            break;
        case ::kvrpcpb::E_Plus:
        case ::kvrpcpb::E_Minus:
        case ::kvrpcpb::E_Mult:
        case ::kvrpcpb::E_Div:
            //TO DO support computing function
            //FLOG_ERROR("Don't support math operation.");
            FLOG_DEBUG("compute...get left value expr_type: %d %p",
                       expr.child(0).expr_type(), &expr.child(0));
            if ((fv = GetValFromExpr(result, expr.child(0))) == nullptr) {
                return nullptr;
            }
            l.reset(fv);
            fv = nullptr;
            FLOG_DEBUG("compute...get right value expr_type: %d %p",
                       expr.child(1).expr_type(), &expr.child(1));
            if ((fv = GetValFromExpr(result, expr.child(1))) == nullptr) {
                return nullptr;
            }
            r.reset(fv);
            fv = nullptr;
            //generate a Const Expr
            if (!(ComputeExpr(expr, l.get(), r.get(), &fv)).ok()) {
                return nullptr;
            }
            break;
        default:
            FLOG_ERROR("GetValFromExpr error, invalid expr_type: %d", expr.expr_type());
            return nullptr;
    }

    //FLOG_DEBUG("GetValFromExpr() fv: %s", fv->ToString().c_str());
    return fv;
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


bool CWhereExpr::FilterExpr(const RowResult &result,
                            const ::kvrpcpb::Expr &expr) {
    auto et = expr.expr_type();
    //static int nCnt{0};
    //FLOG_DEBUG("FilterExpr() %d)...... expr_type: %d  %p", ++nCnt, et, &expr);

    if (!check(expr)) {
        FLOG_ERROR("FilterExpr() error, passin invalid Expr parameter."
                   "expr_type: %d child_size: %d", expr.expr_type(), expr.child_size());
        return false;
    }

    FieldValue* l = nullptr;
    FieldValue* r = nullptr;
    ::kvrpcpb::Expr tmp;

    bool bl, br;
    switch (et) {
        case ::kvrpcpb::E_LogicAnd:
            bl = FilterExpr(result, expr.child(0));
            if (!bl) {
                return false;
            }
            br = FilterExpr(result, expr.child(1));
            return br;
        case ::kvrpcpb::E_LogicOr:
            bl = FilterExpr(result, expr.child(0));
            if (bl) {
                return bl;
            }
            br = FilterExpr(result, expr.child(1));
            return br;
        case ::kvrpcpb::E_LogicNot:
            return (!FilterExpr(result, expr.child(0)));
        case ::kvrpcpb::E_Equal:
        case ::kvrpcpb::E_NotEqual:
        case ::kvrpcpb::E_Less:
        case ::kvrpcpb::E_LessOrEqual:
        case ::kvrpcpb::E_Larger:
        case ::kvrpcpb::E_LargerOrEqual:
            //TO DO support nest expression, such as "a == b + 1:
            l = GetValFromExpr(result, expr.child(0));
            r = GetValFromExpr(result, expr.child(1));
            if (l == nullptr || r == nullptr) {
                FLOG_ERROR("FilterExpr() error, expr type: %d  need two operators", et);
                return false;
            }

            FLOG_DEBUG(">>>parent expr_type: %d get left value expr_type: %d addr: %p  value: %s",
                       et, expr.child(0).expr_type(), &expr.child(0), l->ToString().c_str());
            FLOG_DEBUG(">>>parent expr_type: %d get right value expr_type: %d addr: %p value: %s",
                       et, expr.child(1).expr_type(), &expr.child(1), r->ToString().c_str());

            bl = CWhereExpr::CmpExpr(l, r, et);
            if (expr.child(0).expr_type() != ::kvrpcpb::E_ExprCol) delete l;
            if (expr.child(1).expr_type() != ::kvrpcpb::E_ExprCol) delete r;

            return bl;
        default:
            FLOG_ERROR("FilterExpr() error, invalid expr type: %d", et);
            return false;
    }
}



}
}
}