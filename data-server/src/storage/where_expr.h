_Pragma("once");

#include <map>
#include <string>
#include <memory>

#include "frame/sf_logger.h"
#include "base/status.h"
#include "proto/gen/kvrpcpb.pb.h"
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

        CWhereExpr(const ::kvrpcpb::MatchExt& match_ext) {
            //TO DO init MatchExt
            if (match_ext.has_expr()) {
                ext_filters_ = std::make_shared<::kvrpcpb::MatchExt>(std::move(match_ext));
                //ext_filters_->CopyFrom(match_ext);

                assert(ext_filters_->expr().child_size() > 0);
            }
        }

        ~CWhereExpr() {};

        //TO DO support sql func on column
        template<class T>
            FieldValue* GetValFromExpr(
                    const T& result,
                    const ::kvrpcpb::Expr& expr) {
                FieldValue *fv = nullptr;
                //std::shared_ptr<FieldValue> l = nullptr;
                //std::shared_ptr<FieldValue> r = nullptr;
                FieldValue *l = nullptr;
                FieldValue *r = nullptr;

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
                            if (expr.child(0).expr_type() != ::kvrpcpb::E_ExprCol) {
                                free(l);
                            }
                            if (expr.child(1).expr_type() != ::kvrpcpb::E_ExprCol) {
                                free(r);
                            }
                            return nullptr;
                        }
                        break;
                    default:
                        FLOG_ERROR("GetValFromExpr error, invalid expr_type: %d", expr.expr_type());
                        return nullptr;
                }


                if (expr.child_size() > 0 && expr.child(0).expr_type() != ::kvrpcpb::E_ExprCol) {
                    free(l);
                }
                if (expr.child_size() == 2 && expr.child(1).expr_type() != ::kvrpcpb::E_ExprCol) {
                    free(r);
                }
                //FLOG_DEBUG("GetValFromExpr() fv: %s", fv->ToString().c_str());
                return fv;
            };

        FieldValue* GetExprVal(const std::string& inVal,
                const metapb::Column& col);

        static bool CmpExpr(const FieldValue* l,
                const FieldValue* r,
                ::kvrpcpb::ExprType et);

        Status ComputeExpr(const ::kvrpcpb::Expr& expr,
                const FieldValue *l,
                const FieldValue *r,
                FieldValue **fv);

        template<class T>
            bool FilterExpr(const T& result,
                    const ::kvrpcpb::Expr& expr) {

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

                bool bResult{false};
                switch (et) {
                    case ::kvrpcpb::E_LogicAnd:
                        for (auto i = 0; i < expr.child_size(); i++) {
                            if (!FilterExpr(result, expr.child(i))) {
                                return false;
                            }
                        }
                        return true;
                    case ::kvrpcpb::E_LogicOr:
                        for (auto i = 0; i < expr.child_size(); i++) {
                            if (FilterExpr(result, expr.child(i))) {
                                return true;
                            }
                        }
                        return false;
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

                        bResult = CWhereExpr::CmpExpr(l, r, et);
                        if (expr.child(0).expr_type() != ::kvrpcpb::E_ExprCol) delete l;
                        if (expr.child(1).expr_type() != ::kvrpcpb::E_ExprCol) delete r;

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

                const ::kvrpcpb::Expr &expr = ext_filters_->expr();
                if (expr.expr_type() == ::kvrpcpb::E_Invalid) {
                    FLOG_ERROR("filter() error, Invalid expr_type: %d", expr.expr_type());
                    return false;
                }

                FLOG_DEBUG("start execute filterExpr() expr_type: %d child_size: %d %p",
                        expr.expr_type(), expr.child_size(), &expr);
                return FilterExpr(result, expr);
            }

        //check parameter
        bool check(const ::kvrpcpb::Expr &e) {
            switch (e.expr_type()) {
                case ::kvrpcpb::E_LogicAnd:
                case ::kvrpcpb::E_LogicOr:
                    if (e.child_size() < 2) {
                        FLOG_WARN("check() expr_type: %d child_size: %d",
                                  e.expr_type(), e.child_size());
                        return false;
                    }
                    return true;
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
    private:
        std::shared_ptr<::kvrpcpb::MatchExt> ext_filters_{nullptr};
};


}
}
}
