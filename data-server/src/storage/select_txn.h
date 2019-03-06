_Pragma("once");

#include "store.h"
#include "proto/gen/txn.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class CWhereExpr;

class TxnRowValue {
public:
    TxnRowValue() = default;
    ~TxnRowValue();

    TxnRowValue(const TxnRowValue&) = delete;
    TxnRowValue& operator=(const TxnRowValue&) = delete;

    uint64_t GetVersion() const { return version_; }
    void SetVersion(uint64_t ver) { version_ = ver; }

    FieldValue* GetField(uint64_t col) const;
    bool AddField(uint64_t col, std::unique_ptr<FieldValue>& field);

    void Encode(const txnpb::SelectRequest& req, txnpb::RowValue* to);

private:
    uint64_t version_ = 0;
    std::map<uint64_t, FieldValue*> fields_;
};

class TxnRowDecoder {
public:
    TxnRowDecoder(const std::vector<metapb::Column>& primary_keys, const txnpb::SelectRequest& req);
    ~TxnRowDecoder() = default;

    TxnRowDecoder(const TxnRowDecoder&) = delete;
    TxnRowDecoder& operator=(const TxnRowDecoder&) = delete;

    Status DecodeAndFilter(const std::string& key, const std::string& buf,
                           TxnRowValue& row, bool& matched);

private:
    Status decodePrimaryKeys(const std::string& key, TxnRowValue& row);
    Status decodeFields(const std::string& buf, TxnRowValue& row);

private:
    const std::vector<metapb::Column>& primary_keys_;
    std::map<uint64_t, metapb::Column> cols_;
    std::vector<kvrpcpb::Match> filters_;
    std::shared_ptr<CWhereExpr>  where_expr_{nullptr};
};

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
    std::unique_ptr<Iterator> data_iter_;
    std::unique_ptr<Iterator> txn_iter_;
    Status last_status_;
    bool over_ = false;
    uint64_t iter_count_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
