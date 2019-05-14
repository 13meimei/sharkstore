_Pragma("once");

#include "store.h"
#include "proto/gen/txn.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class CWhereExpr;

class TxnRowFetcher {
public:
    TxnRowFetcher(Store& s, const txnpb::SelectRequest& req);

    virtual ~TxnRowFetcher() = default;

    TxnRowFetcher(const TxnRowFetcher&) = delete;
    TxnRowFetcher& operator=(const TxnRowFetcher&) = delete;

    virtual Status Next(txnpb::Row& row, bool& over) = 0;

protected:
    Status getRow(const std::string& key, const std::string& data_value,
            const std::string& intent_value, txnpb::Row& row);

private:
    // add data from default cf
    Status addDefault(const std::string& key, const std::string& buf, txnpb::Row& row);
    Status addIntent(const txnpb::TxnValue& txn_value, txnpb::Row& row);

protected:
    Store& store_;
    const txnpb::SelectRequest& req_;
    TxnRowDecoder decoder_;
};

std::unique_ptr<TxnRowFetcher> NewTxnRowFetcher(Store& s, const txnpb::SelectRequest& req);

class PointRowFetcher : public TxnRowFetcher {
public:
    PointRowFetcher(Store& s, const txnpb::SelectRequest& req);
    ~PointRowFetcher() = default;

    Status Next(txnpb::Row& row, bool& over) override;

private:
    bool fetched_ = false;
};

class RangeRowFetcher : public TxnRowFetcher {
public:
    RangeRowFetcher(Store& s, const txnpb::SelectRequest& req);
    ~RangeRowFetcher() = default;

    Status Next(txnpb::Row& row, bool& over) override;

private:
    bool tryGetRow(txnpb::Row& row);
    bool checkIterValid();

private:
    std::unique_ptr<IteratorInterface> data_iter_;
    std::unique_ptr<IteratorInterface> txn_iter_;
    Status last_status_;
    bool over_ = false;
    uint64_t iter_count_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
