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
class RowResult;
class RowDecoder;

//bool check(const ::kvrpcpb::Expr &e);

class CWhereExpr {
public:
    CWhereExpr() = delete;

    CWhereExpr(const ::kvrpcpb::MatchExt& match_ext) {
        //TO DO init MatchExt
        if (match_ext.has_expr()) {
            ext_filters_ = std::make_shared<::kvrpcpb::MatchExt>();
            ext_filters_->CopyFrom(match_ext);

            assert(ext_filters_->expr().child_size() > 0);
            //ext_filters_.reset(const_cast<::kvrpcpb::MatchExt *>(&match_ext));
        }
    }

    ~CWhereExpr() {};

    //TO DO support sql func on column
    FieldValue* GetValFromExpr(
            const RowResult &result,
            const ::kvrpcpb::Expr &expr);

    FieldValue* GetExprVal(const std::string &inVal,
                           const metapb::Column &col);

    static bool CmpExpr(const FieldValue* l,
                        const FieldValue* r,
                        ::kvrpcpb::ExprType et);

    Status ComputeExpr(const ::kvrpcpb::Expr& expr,
                       const FieldValue *l,
                       const FieldValue *r,
                        FieldValue **fv);

    bool FilterExpr(const RowResult &result,
                    const ::kvrpcpb::Expr &expr);

    //filter entrance
    bool Filter(const RowResult &result) {
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

private:
    std::shared_ptr<::kvrpcpb::MatchExt> ext_filters_{nullptr};
};


}
}
}