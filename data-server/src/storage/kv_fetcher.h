_Pragma("once");

#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

struct KvRecord {
    enum Flags: uint32_t {
        kHasValue = 1U << 0,
        kHasIntent = 1U << 1,
    };

    std::string key;
    std::string value;
    std::string intent;
    uint32_t flag = 0;

    void MarkHasValue() { flag |= Flags::kHasValue; }
    bool HasValue() const { return (flag & Flags::kHasValue) != 0; }

    void MarkHasIntent() { flag |= Flags::kHasIntent; }
    bool HasIntent() const { return (flag & Flags::kHasIntent) != 0; }

    void Clear() { flag = 0; }
    bool Valid() const { return HasValue() || HasIntent(); }
};


class KvFetcher {
public:
    KvFetcher() = default;
    virtual ~KvFetcher() = default;

    // 如果Status非ok活着非rec.Valid则表示结束
    virtual Status Next(KvRecord& rec) = 0;

    static std::unique_ptr<KvFetcher> Create(Store& store, const kvrpcpb::SelectRequest& req);
    static std::unique_ptr<KvFetcher> Create(Store& store, const kvrpcpb::DeleteRequest& req);
    static std::unique_ptr<KvFetcher> Create(Store& store, const txnpb::SelectRequest& req);
};


class PointerKvFetcher : public KvFetcher {
public:
    PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent);

    Status Next(KvRecord& rec) override;

private:
    Store& store_;
    const std::string& key_;
    bool fetch_intent_ = false;
    bool fetched_ = false;
};


class RangeKvFetcher : public KvFetcher {
public:
    RangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    std::unique_ptr<IteratorInterface> iter_ = nullptr;
    Status status_;
};


class TxnRangeKvFetcher : public KvFetcher {
public:
    TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    bool valid();

private:
    std::unique_ptr<IteratorInterface> data_iter_;
    std::unique_ptr<IteratorInterface> txn_iter_;
    bool over_ = false;
    Status status_;
};

} // namespace storage
} // namespace dataserver
} // namespace sharkstore
