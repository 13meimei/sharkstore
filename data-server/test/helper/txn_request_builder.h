_Pragma("once");

/**
 * use for testing txn function declared in store.h
 *  TXN
    void TxnPrepare(RPCRequestPtr rpc, txnpb::DsPrepareRequest& req);
    void TxnDecide(RPCRequestPtr rpc, txnpb::DsDecideRequest& req);
    void TxnClearup(RPCRequestPtr rpc, txnpb::DsClearupRequest& req);
    void TxnGetLockInfo(RPCRequestPtr rpc, txnpb::DsGetLockInfoRequest& req);
    void TxnSelect(RPCRequestPtr rpc, txnpb::DsSelectRequest& req);
 */

#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/txn.pb.h"

#include "table.h"
#include "request_builder.h"

namespace sharkstore {
namespace test {
namespace helper {


class TxnSelectRequestBuilder {
public:
    TxnSelectRequestBuilder(Table *t) : table_(t) {
        ori_req_ = std::make_shared<SelectRequestBuilder>(t);
        // default select all scope
        ori_req_->SetScope({}, {});

        auto tmp = ori_req_->Build();

        req_.set_key(tmp.key());
        req_.mutable_scope()->CopyFrom(tmp.scope());
        req_.mutable_field_list()->CopyFrom(tmp.field_list());
        req_.mutable_limit()->CopyFrom(tmp.limit());
        req_.clear_ext_filter();
    }

    void AddAllFields();

    txnpb::SelectRequest Build() {
        return std::move(req_);
    }

private:
    Table *table_ = nullptr;
    txnpb::SelectRequest req_;
    std::shared_ptr<SelectRequestBuilder> ori_req_;
};

class PrepareRequestBuilder {
public:
    explicit PrepareRequestBuilder() {};

    //
    void SetTxnId(const std::string& tid = "1") noexcept {
        req_.set_txn_id(tid);
    }
    void SetPrimaryKey(const std::string& pkey) {
        req_.set_primary_key(pkey);
    }
    void SetLockTtl(const uint64_t ttl) noexcept {
        req_.set_lock_ttl(ttl);
    }
    void SetStrictCheck(bool flag = false) {
        req_.set_strict_check(flag);
    }
    void SetLocal(bool flag = false) {
        req_.set_local(flag);
    }
    void AppendSecondKeys(const std::string& key) {
        req_.add_secondary_keys(std::move(key));
    }

    static txnpb::TxnIntent* CreateTxnIntent(txnpb::OpType op, const std::string& key,
                                      const std::string& value, bool check_unique,
                                      uint64_t exp_ver, bool is_primary);

    void AppendIntents(const txnpb::TxnIntent* tnt);

    // select multi rows
    void SetScope(const std::vector<std::string>& start_pk_values,
            const std::vector<std::string>& end_pk_values);

    void ClearSecondKeys() noexcept {
        req_.clear_secondary_keys();
    }

    txnpb::PrepareRequest Build() { return std::move(req_); }

private:
    txnpb::PrepareRequest req_;
};

class DecideRequestBuilder {
public:
    explicit DecideRequestBuilder() {};

    void SetTxnId(const std::string& tid = "1") noexcept {
        req_.set_txn_id(tid);
    }

    void setTxnStatus(txnpb::TxnStatus sts) noexcept {
        req_.set_status(sts);
    }

    void AppendKeys(const std::string& key) {
        req_.add_keys(std::move(key));
    }

    void SetRecover(bool flag = false) noexcept {
        req_.set_recover(flag);
    }
    void setIsPrimary(bool flag = false) noexcept {
        req_.set_is_primary(flag);
    }

    txnpb::DecideRequest Build() { return std::move(req_); }

private:
    txnpb::DecideRequest req_;
};

/**
 * message GetLockInfoRequest {¬
        bytes key = 1;
        string txn_id = 2; // 事务ID为空，则返回当前lock
    }
 */
class GetLockInfoRequestBuilder {
public:
    explicit GetLockInfoRequestBuilder() {};

    void SetKey(const std::string& key) noexcept {
        req_.set_key(key);
    }

    void SetTxnId(const std::string& tid = "1") noexcept {
        req_.set_txn_id(tid);
    }

    txnpb::GetLockInfoRequest Build() { return std::move(req_); }

private:
    //Table *table_ = nullptr;
    txnpb::GetLockInfoRequest req_;
};

/**
 * // 删除最后的primary row
    message ClearupRequest {
        string txn_id       = 1;
        bytes primary_key   = 2;
    }
 */
class ClearupRequestBuilder {
public:
    explicit ClearupRequestBuilder() {};

    void SetKey(const std::string& key) noexcept {
        req_.set_primary_key(key);
    }

    void SetTxnId(const std::string& tid = "1") noexcept {
        req_.set_txn_id(tid);
    }

    txnpb::ClearupRequest Build() { return std::move(req_); }

private:
    //Table *table_ = nullptr;
    txnpb::ClearupRequest req_;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
