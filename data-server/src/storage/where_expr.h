_Pragma("once");

#include <map>
#include <string>
#include <memory>

#include "frame/sf_logger.h"
#include "base/status.h"
#include "proto/gen/exprpb.pb.h"
#include "proto/gen/metapb.pb.h"
#include "field_value.h"
#include "row_decoder.h"

namespace sharkstore {
class Status;
namespace dataserver {
namespace storage {

class FieldValue;

class CWhereExpr {
public:
    CWhereExpr() = delete;

    CWhereExpr(const ::exprpb::Expr& match_ext) {
        ext_filters_ = std::make_shared<::exprpb::Expr>(match_ext);
    }

    ~CWhereExpr() {};

    //TO DO support sql func on column
    template<class T>
    FieldValue* GetValFromExpr(const T& result, const ::exprpb::Expr& expr) {
        FieldValue *fv = nullptr;
        //std::shared_ptr<FieldValue> l = nullptr;
        //std::shared_ptr<FieldValue> r = nullptr;
        FieldValue *l = nullptr;
        FieldValue *r = nullptr;

        switch (expr.expr_type()) {
            case ::exprpb::Column:
                fv = result.GetField(expr.column().id());
                break;
            case ::exprpb::Const_Int:
            case ::exprpb::Const_UInt:
            case ::exprpb::ExprConst:
                fv = GetExprVal(expr.value(), expr.column());
                break;
            case ::exprpb::Plus:
            case ::exprpb::Minus:
            case ::exprpb::Mult:
            case ::exprpb::Div:
                //TO DO support computing function
                //FLOG_ERROR("Don't support math operation.");
                FLOG_DEBUG("compute...get left value expr_type: %d %p",
                        expr.child(0).expr_type(), &expr.child(0));
                if ((l = GetValFromExpr(result, expr.child(0))) == nullptr) {
                    return nullptr;
                }

                FLOG_DEBUG("compute...get right value expr_type: %d %p",
                        expr.child(1).expr_type(), &expr.child(1));
                if ((r = GetValFromExpr(result, expr.child(1))) == nullptr) {
                    return nullptr;
                }

                //generate a Const Expr
                if (!(ComputeExpr(expr, l, r, &fv)).ok()) {
                    if (expr.child(0).expr_type() != ::exprpb::ExprCol) {
                        free(l);
                    }
                    if (expr.child(1).expr_type() != ::exprpb::ExprCol) {
                        free(r);
                    }
                    return nullptr;
                }
                break;
            default:
                FLOG_ERROR("GetValFromExpr error, invalid expr_type: %d", expr.expr_type());
                return nullptr;
        }

        if (expr.child_size() > 0 && expr.child(0).expr_type() != ::exprpb::Column) {
            free(l);
        }
        if (expr.child_size() == 2 && expr.child(1).expr_type() != ::exprpb::Column) {
            free(r);
        }
        return fv;
    };

    FieldValue* GetExprVal(const std::string& inVal, const metapb::Column& col);

    static bool CmpExpr(const FieldValue* l, const FieldValue* r, ::exprpb::ExprType et);

    Status ComputeExpr(const ::exprpb::Expr& expr, const FieldValue *l, const FieldValue *r, FieldValue **fv);

    template<class T>
    bool FilterExpr(const T& result, const ::exprpb::Expr& expr) {
        auto et = expr.expr_type();

        if (!check(expr)) {
            FLOG_ERROR("FilterExpr() error, passin invalid Expr parameter."
                    "expr_type: %d child_size: %d", expr.expr_type(), expr.child_size());
            return false;
        }

        FieldValue* l = nullptr;
        FieldValue* r = nullptr;
        ::exprpb::Expr tmp;

        bool bResult{false};
        switch (et) {
            case ::exprpb::LogicAnd:
                for (auto i = 0; i < expr.child_size(); i++) {
                    if (!FilterExpr(result, expr.child(i))) {
                        return false;
                    }
                }
                return true;
            case ::exprpb::LogicOr:
                for (auto i = 0; i < expr.child_size(); i++) {
                    if (FilterExpr(result, expr.child(i))) {
                        return true;
                    }
                }
                return false;
            case ::exprpb::LogicNot:
                return (!FilterExpr(result, expr.child(0)));
            case ::exprpb::Equal:
            case ::exprpb::NotEqual:
            case ::exprpb::Less:
            case ::exprpb::LessOrEqual:
            case ::exprpb::Larger:
            case ::exprpb::LargerOrEqual:
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

                bResult = CWhereExpr::CmpExpr(l, r, et);
                if (expr.child(0).expr_type() != ::exprpb::Column) delete l;
                if (expr.child(1).expr_type() != ::exprpb::Column) delete r;

                return bResult;
            default:
                FLOG_ERROR("FilterExpr() error, invalid expr type: %d", et);
                return false;
        }
    }

    //filter entrance
    template<class T>
    bool Filter(const T& result) {
        if (!ext_filters_->has_expr()) {
            FLOG_WARN("filter() no expr restriction");
            return true;
        }

        const ::exprpb::Expr &expr = ext_filters_->expr();
        if (expr.expr_type() == ::exprpb::Invalid) {
            FLOG_ERROR("filter() error, Invalid expr_type: %d", expr.expr_type());
            return false;
        }

        FLOG_DEBUG("start execute filterExpr() expr_type: %d child_size: %d %p",
                expr.expr_type(), expr.child_size(), &expr);
        return FilterExpr(result, expr);
    }

    //check parameter
    bool check(const ::exprpb::Expr &e) {
        switch (e.expr_type()) {
            case ::exprpb::LogicAnd:
            case ::exprpb::LogicOr:
                if (e.child_size() < 2) {
                    FLOG_WARN("check() expr_type: %d child_size: %d",
                            e.expr_type(), e.child_size());
                        return false;
                }
                return true;
            case ::exprpb::Equal:
            case ::exprpb::NotEqual:
            case ::exprpb::Less:
            case ::exprpb::LessOrEqual:
            case ::exprpb::Larger:
            case ::exprpb::LargerOrEqual:
            case ::exprpb::Plus:
            case ::exprpb::Minus:
            case ::exprpb::Mult:
            case ::exprpb::Div:
                if (e.child_size() != 2) {
                    FLOG_WARN("check() expr_type: %d child_size: %d",
                            e.expr_type(), e.child_size());
                    return false;
                }
                return true;
            case ::exprpb::LogicNot:
                if (e.child_size() != 1) {
                    return false;
                }
                return true;
            case ::exprpb::Column: //leaf node
            case ::exprpb::Const:
                if (e.child_size() != 0) {
                    return false;
                }
                return true;
            default:
                FLOG_ERROR("check() error, INVALID expr_type");
                return false;
        }
    }

private:
    std::shared_ptr<::exprpb::Expr> ext_filters_{nullptr};
};


}
}
}
