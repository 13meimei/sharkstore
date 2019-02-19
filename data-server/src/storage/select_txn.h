_Pragma("once");

#include "store.h"
#include "proto/gen/txn.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

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
};

class TxnRowFetcher {
public:
    TxnRowFetcher(Store& s, const txnpb::SelectRequest& req);
    ~TxnRowFetcher();

    TxnRowFetcher(const TxnRowFetcher&) = delete;
    TxnRowFetcher& operator=(const TxnRowFetcher&) = delete;

    Status Next(txnpb::Row& row, bool& over);

private:
    Store& store_;
    TxnRowDecoder decoder_;
    Iterator* data_iter_ = nullptr;
    Iterator* txn_iter_ = nullptr;
};


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
