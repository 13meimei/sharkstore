#include "request_builder.h"
#include "txn_request_builder.h"

#include "base/util.h"
#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

using sharkstore::randomInt;

static std::string buildKey(Table *table, const std::vector<std::string>& values) {
    auto pks = table->GetPKs();
    if (values.size() != pks.size()) {
        throw std::runtime_error("mismatched table primary keys count and input values count");
    }

    std::string buf;
    EncodeKeyPrefix(&buf, table->GetID());
    for (size_t i = 0; i < pks.size(); ++i) {
        EncodePrimaryKey(&buf, pks[i], values[i]);
    }
    return buf;
}

static std::pair<std::string, std::string> buildScope(Table *table,
                                                      const std::vector<std::string>& start_values,
                                                      const std::vector<std::string>& end_values) {
    auto pks = table->GetPKs();
    std::string start, end;

    EncodeKeyPrefix(&start, table->GetID());
    auto len = std::min(pks.size(), start_values.size());
    for (size_t i = 0; i < len; ++i) {
        EncodePrimaryKey(&start, pks[i], start_values[i]);
    }

    if (end_values.empty()) {
        EncodeKeyPrefix(&end, table->GetID() + 1);
    } else {
        EncodeKeyPrefix(&end, table->GetID());
        auto len = std::min(pks.size(), end_values.size());
        for (size_t i = 0; i < len; ++i) {
            EncodePrimaryKey(&end, pks[i], end_values[i]);
        }
    }

    return std::make_pair(start, end);
}


SelectRequestBuilder::SelectRequestBuilder(Table *t) : table_(t) {
    // default select all scope
    SetScope({}, {});
}

void SelectRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

void SelectRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
              const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}

void SelectRequestBuilder::AddField(const std::string& col_name) {
    auto f = req_.add_field_list();
    f->set_typ(kvrpcpb::SelectField_Type_Column);
    f->mutable_column()->CopyFrom(table_->GetColumn(col_name));
}

void SelectRequestBuilder::AddAllFields() {
    auto cols = table_->GetAllColumns();
    for (const auto& col: cols) {
        auto f = req_.add_field_list();
        f->set_typ(kvrpcpb::SelectField_Type_Column);
        f->mutable_column()->CopyFrom(col);
    }
}

std::vector<metapb::Column> SelectRequestBuilder::AddRandomFields(size_t size) {
    std::vector<metapb::Column> field_lists;
    auto cols = table_->GetAllColumns();
    if (size == 0) {
        size = 1 + (sharkstore::randomInt() % cols.size()) * 2;
    }
    for (size_t i = 0; i < size; ++i) {
        auto idx = sharkstore::randomInt() % cols.size();
        auto f = req_.add_field_list();
        f->set_typ(kvrpcpb::SelectField_Type_Column);
        f->mutable_column()->CopyFrom(cols[idx]);
        field_lists.push_back(cols[idx]);
    }
    return field_lists;
}

void SelectRequestBuilder::AddAggreFunc(const std::string& func_name, const std::string& col_name) {
    auto f = req_.add_field_list();
    f->set_typ(kvrpcpb::SelectField_Type_AggreFunction);
    f->mutable_aggre_func()->assign(func_name);
    if (!col_name.empty()) {
        f->mutable_column()->CopyFrom(table_->GetColumn(col_name));
    }
}

void SelectRequestBuilder::AddMatch(const std::string& col, kvrpcpb::MatchType type, const std::string& val) {
    auto w = req_.add_where_filters();
    w->set_match_type(type);
    w->mutable_column()->CopyFrom(table_->GetColumn(col));
    w->mutable_threshold()->assign(val);
}

static ::kvrpcpb::Expr *CreateExprCol(const metapb::Column &col, const std::string &name, ::kvrpcpb::Expr* e) {

//    printf(">>>>>>>>>leaf expr_type: %d\n", kvrpcpb::E_ExprCol);
    e->mutable_column()->CopyFrom(col);
    e->set_expr_type(kvrpcpb::E_ExprCol);
    e->mutable_column()->set_name(name);

    return e;
}

static ::kvrpcpb::Expr *CreateExprVal(const metapb::Column& col, const std::string &val, ::kvrpcpb::Expr* e) {

//    printf(">>>>>>>>>leaf expr_type: %d\n", kvrpcpb::E_ExprConst);
    e->set_expr_type(kvrpcpb::E_ExprConst);
    e->set_value(val);
    e->mutable_column()->CopyFrom(col);

    return e;
}

static ::kvrpcpb::Expr *CreateExpr(::kvrpcpb::Expr *e, const metapb::Column &col, const std::string& name,
        const std::string& val, ::kvrpcpb::ExprType et)
{
    //printf(">>>>>>child expr_type: %d\n", et);
    e->set_expr_type(et);

    auto l = e->add_child();
    CreateExprCol(col, name, l);

    auto r = e->add_child();
    CreateExprVal(col, val, r);
    return e;
}

static int  buildMathExpr(const metapb::Column& col, const std::string &flag, kvrpcpb::Expr *e) {
    kvrpcpb::Expr *l = nullptr;
    kvrpcpb::Expr *r = nullptr;
    std::string value;
    std::string el;
    std::string er;
    size_t  pos;

    //printf("buildMathExpr.....flag:%s\n", flag.c_str());

    //char ff = *(const_cast<char*>(flag.c_str()));
    char ff = *(flag.data());
    switch (ff) {
        case '+':
            e->set_expr_type(kvrpcpb::E_Plus);
            break;
        case '-':
            e->set_expr_type(kvrpcpb::E_Minus);
            break;
        case '*':
            e->set_expr_type(kvrpcpb::E_Mult);
            break;
        case '/':
            e->set_expr_type(kvrpcpb::E_Div);
            break;
        default:
            return -1;
    }

    value.swap(*(e->mutable_value()));
    pos = value.find(flag);
    if (pos == std::string::npos) {
        fprintf(stderr, "%s:%d find flag: %s failure.\n", __FILE__, __LINE__, flag.c_str());
        return -1;
    }

    el = value.substr(0, pos);
    er = value.substr(pos+1);
    if (el == col.name()) {
        l = e->add_child();
        auto tmp = new metapb::Column(col);
        l->set_allocated_column(tmp);
        l->set_expr_type(kvrpcpb::E_ExprCol);
    } else {
        l = e->add_child();
        l->mutable_column()->CopyFrom(col);
        l->set_value(el);
        l->set_expr_type(kvrpcpb::E_ExprConst);
    }

    if (er == col.name()) {
        r = e->add_child();
        auto column = new metapb::Column(col);
        r->set_allocated_column(column);
        r->set_expr_type(kvrpcpb::E_ExprCol);
    } else {
        r = e->add_child();
        r->mutable_column()->CopyFrom(col);
        r->set_value(er);
        r->set_expr_type(kvrpcpb::E_ExprConst);
    }

    return 0;
}
//where id = 1 + 1
//col = id
//val = 1 + 1
//decode val into Expr
void SelectRequestBuilder::AppendCompCond(const std::string& col, const std::string& val,
        ::kvrpcpb::ExprType et, ::kvrpcpb::ExprType logic_suffix)
{
    AppendMatchExt(col, val, et, logic_suffix);
    //printf("AppendCompCond...\n");

    auto root = req_.mutable_ext_filter()->mutable_expr();
    decltype(root) l = nullptr, r = nullptr;

    const metapb::Column& column = table_->GetColumn(col);
    auto tmp = root;

    //change const Expr id+1 to 3 Expr,like + and id and 1
    auto fn = [&](kvrpcpb::Expr *l) ->void {
        //printf("in lambda....%s\n", l->value().c_str());

        if (l->expr_type() == kvrpcpb::E_ExprConst &&
            l->column().data_type() >= metapb::Tinyint &&
            l->column().data_type() <= metapb::Double)
        {
                if (l->value().find("+") != std::string::npos) {
                    //to do decode +
                    buildMathExpr(column, "+", l);
                    return;
                }
                if (l->value().find("-") != std::string::npos) {
                    //to do decode -
                    buildMathExpr(column, "-", l);
                    return;
                }
                if (l->value().find("*") != std::string::npos) {
                    //to do decode *
                    buildMathExpr(column, "*", l);
                    return;
                }
                if (l->value().find("/") != std::string::npos) {
                    //to do decode /
                    buildMathExpr(column, "/", l);
                    return;
                }
        } //end if
    };

    bool lend{false};
    bool rend{false};
    int idx{0};

    auto set_flag = [&]() {
        if (!lend) {
            lend = true;
            tmp = root;
        } else {
            rend = true;
            tmp = root;
        }
    };

    while (tmp->child_size() > 0) {
        if (lend && rend) break;

        if (!lend) {
            idx = 0;
        } else if (tmp->child_size() == 2){
            idx = 1;
        } else {
            idx = 0;
        }
        //Not releation expr
        if (tmp->expr_type() < ::kvrpcpb::E_Equal ||
                tmp->expr_type() > ::kvrpcpb::E_LargerOrEqual)
        {
            //printf("AppendCompCond...continue\n");
            if (tmp->child_size() == 0) {
                set_flag();
            } else {
                tmp = tmp->mutable_child(idx);
            }
            continue;
        }

        l = tmp->mutable_child(0);
        fn(l);
        r = tmp->mutable_child(1);
        fn(r);

        //TO DO  next expr
        set_flag();
    } //end while
}

//
//support append simple condition such as where id = 1 or id = 2
//Not support where id = 1 + 1 and this series condition will support in other func
//logic_suffix: logic relation with previous expression
//              first append represent for root
void SelectRequestBuilder::AppendMatchExt(const std::string& col, const std::string& val,
        ::kvrpcpb::ExprType et, ::kvrpcpb::ExprType logic_suffix)
{
    auto root = req_.mutable_ext_filter()->mutable_expr();

    //parent expr
    ::kvrpcpb::Expr *pe = nullptr;
    auto first = false;

    if (logic_suffix == 0 && (et < 11 || et > 16)) {
        fprintf(stderr, "%s:%d must be relation expr_type if last expr!\n",__FILE__, __LINE__);
        return;
    }

    pe = root;
    //child expr
    if (pe->child_size() < 2) {
        decltype(pe) l{nullptr};

        if (pe->child_size() == 0)
        {
            l = pe;
            if (logic_suffix > 0) {
            //    printf("root logic expr_type: %d\n", logic_suffix);
                pe->set_expr_type(logic_suffix);
                l = pe->add_child();
                first = true;
            }
        }

        if (pe->child_size() == 1 && !first) {
        //    printf(">>>child logic expr_type: %d\n", logic_suffix);
            l = pe->add_child();
            if (logic_suffix > 0) {
                l->set_expr_type(logic_suffix);
                l = l->add_child();
            }
        }
        auto tmp = CreateExpr(l, table_->GetColumn(col), col, val, et);
        return;
    }

    int ts{0};
    int idx{0};
    decltype(pe) tr = nullptr;

    assert(pe->child_size() == 2);

    while ((ts = pe->child_size()) > 0) {
        idx = 0;
        if (ts == 1) {
            auto l = pe->add_child();
            if (logic_suffix > 0) {
                l->set_expr_type(logic_suffix);
                l = l->add_child();
            }
            CreateExpr(l, table_->GetColumn(col), col, val, et);
         //   printf("in cycle,,, CreateExpr child expr_type: %d \n", l->expr_type());
            return;
        }

        for (auto i=0; i<ts; i++) {
            tr = pe->mutable_child(i);
            if (tr->child_size() < 2) {
                auto l = tr->add_child();
                if (logic_suffix > 0) {
            //        printf(">>>in cycle child expr_type: %d\n", logic_suffix);
                    l->set_expr_type(logic_suffix);
                    l = l->add_child();
                }
                CreateExpr(l, table_->GetColumn(col), col, val, et);
            //    printf("in cycle, CreateExpr child expr_type: %d \n", l->expr_type());
                return;
            }
            //printf("%d)child_size: %d ts: %d logic suffix: %d\n", i, tr->child_size(), ts, logic_suffix);

            if (tr->expr_type() == kvrpcpb::E_LogicOr ||
                    tr->expr_type() == kvrpcpb::E_LogicAnd)
            {
            //    printf("encourter logic child: %d ts: %d\n", i, ts);
                idx = i;
                break;
            }
        }
        //printf("in cycle, idx: %d pe->child_size: %d\n", idx, ts);
        if (tr != nullptr) pe = tr->mutable_child(idx);
        else pe = pe->mutable_child(0);
    }
    printf("abnormal end...%d\n", et);
    return;
}

void SelectRequestBuilder::AddLimit(uint64_t count, uint64_t offset) {
    req_.mutable_limit()->set_count(count);
    req_.mutable_limit()->set_offset(offset);
}


DeleteRequestBuilder::DeleteRequestBuilder(Table *t) : table_(t) {
    // default: delete all scope
    SetScope({}, {});
}

// delete one row
void DeleteRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

// detete multi rows
void DeleteRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
                                    const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}

// select where filter
void DeleteRequestBuilder::AddMatch(const std::string& col,
                                    kvrpcpb::MatchType type,
                                    const std::string& val) {
    auto w = req_.add_where_filters();
    w->set_match_type(type);
    w->mutable_column()->CopyFrom(table_->GetColumn(col));
    w->mutable_threshold()->assign(val);
}



InsertRequestBuilder::InsertRequestBuilder(Table *t) : table_(t) {
    pk_columns_ = t->GetPKs();
    non_pk_columns_ = t->GetNonPkColumns();
}

void InsertRequestBuilder::AddRow(const std::vector<std::string>& values) {
    if (values.size() != pk_columns_.size() + non_pk_columns_.size()) {
        throw std::runtime_error("mismatched row values size with table columns size");
    }

    std::string key;
    std::string value;
    size_t index = 0;

    // encode key
    EncodeKeyPrefix(&key, table_->GetID());
    for (const auto &pk : pk_columns_) {
        EncodePrimaryKey(&key, pk, values[index++]);
    }

    // encode value
    for (const auto &col : non_pk_columns_) {
       EncodeColumnValue(&value, col, values[index++]);
    }
    assert(index == values.size());

    auto kv = req_.add_rows();
    kv->set_key(std::move(key));
    kv->set_value(std::move(value));
}

void InsertRequestBuilder::AddRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row: rows) {
        AddRow(row);
    }
}

void InsertRequestBuilder::SetCheckDuplicate() {
    req_.set_check_duplicate(true);
}


UpdateRequestBuilder::UpdateRequestBuilder(Table *t): table_(t) {
    // default select all scope
    SetScope({}, {});
}

// update one row
void UpdateRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

// update multi rows
void UpdateRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
              const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}

// update where filter
void UpdateRequestBuilder::AddMatch(const std::string& col, kvrpcpb::MatchType type, const std::string& val) {
    auto w = req_.add_where_filters();
    w->set_match_type(type);
    w->mutable_column()->CopyFrom(table_->GetColumn(col));
    w->mutable_threshold()->assign(val);
}

// update set value
void UpdateRequestBuilder::SetField(const std::string& col, kvrpcpb::FieldType type, const std::string& val) {
    auto f = req_.add_fields();
    f->mutable_column()->CopyFrom(table_->GetColumn(col));
    f->mutable_value()->assign(val);
    f->set_field_type(type);
}

/*
message TxnIntent {
    OpType typ          = 1;  //INSERT  DELETE
    bytes key           = 2; // 编码后的主键
    bytes value         = 3; // 编码后的列值（所有非主键列）
    bool  check_unique  = 4; // for unique index or insert check pk duplicate
    uint64 expected_ver = 5; // 为0表示不校验version
    bool is_primary     = 6; // 是否是primary row
}*/
txnpb::TxnIntent* PrepareRequestBuilder::CreateTxnIntent(
        txnpb::OpType op, const std::string& key,
        const std::string& value, bool check_unique,
        uint64_t exp_ver, bool is_primary)
{
    txnpb::TxnIntent* tnt = new txnpb::TxnIntent;
    if (tnt == nullptr) {
        return nullptr;
    }
    tnt->set_typ(op);
    tnt->set_key(std::move(key));
    tnt->set_value(std::move(value));
    tnt->set_check_unique(check_unique);
    tnt->set_expected_ver(exp_ver);
    tnt->set_is_primary(is_primary);
    return tnt;
}

void PrepareRequestBuilder::AppendIntents(const txnpb::TxnIntent* tnt) {
    auto i = req_.mutable_intents();
    i->AddAllocated(const_cast<txnpb::TxnIntent*>(tnt));
}

void TxnSelectRequestBuilder::AddAllFields() {
    auto cols = table_->GetAllColumns();
    for (const auto& col: cols) {
        auto f = req_.add_field_list();
        f->set_typ(kvrpcpb::SelectField_Type_Column);
        f->mutable_column()->CopyFrom(col);
    }
}


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */

