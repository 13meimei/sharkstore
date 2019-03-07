_Pragma("once");

#include <stdint.h>
#include <string>
#include <vector>
#include <proto/gen/kvrpcpb.pb.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

enum class FieldType : char {
    kInt,
    kUInt,
    kFloat,
    kBytes,
};

struct FieldUpdate {
    FieldUpdate(uint64_t column_id, uint64_t offset, uint64_t length):
            column_id_(column_id), offset_(offset), length_(length) {
    }

    uint64_t column_id_;
    uint64_t offset_;
    uint64_t length_;
};

class FieldValue {
public:
    explicit FieldValue(int64_t val) : type_(FieldType::kInt) {
        value_.ival = val;
    }
    explicit FieldValue(uint64_t val) : type_(FieldType::kUInt) {
        value_.uval = val;
    }
    explicit FieldValue(double val) : type_(FieldType::kFloat) {
        value_.fval = val;
    }
    explicit FieldValue(std::string* val) : type_(FieldType::kBytes) {
        value_.sval = val;
    }
    explicit FieldValue(const std::string& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(val);
    }
    explicit FieldValue(std::string&& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(std::move(val));
    }

    ~FieldValue() {
        if (FieldType::kBytes == type_) delete value_.sval;
    }

    FieldValue(const FieldValue&) = delete;
    FieldValue& operator=(const FieldValue&) = delete;

    FieldType Type() const { return type_; }

    int64_t Int() const {
        if (type_ != FieldType::kInt) return 0;
        return value_.ival;
    }

    uint64_t UInt() const {
        if (type_ != FieldType::kUInt) return 0;
        return value_.uval;
    }

    double Float() const {
        if (type_ != FieldType::kFloat) return 0;
        return value_.fval;
    }

    const std::string& Bytes() const {
        if (type_ != FieldType::kBytes || value_.sval == nullptr)
            return kDefaultBytes;
        return *value_.sval;
    }

    const std::string ToString() const {
        if (type_ == FieldType::kInt) {
            return std::to_string(value_.ival);
        } else if (type_ == FieldType::kUInt) {
            return std::to_string(value_.uval);
        } else if (type_ == FieldType::kFloat) {
            return std::to_string(value_.fval);
        } else if (type_ == FieldType::kBytes) {
            return *value_.sval;
        } else {
            return "Unknown field value";
        }
    }
public:
    void AssignInt(int64_t v)          { if (type_ == FieldType::kInt)      value_.ival = v; }
    void AssignUint(int64_t v)         { if (type_ == FieldType::kUInt)     value_.uval = v; }
    void AssignFloat(double v)           { if (type_ == FieldType::kFloat)  value_.fval = v; }
    void AssignBytes(std::string* v)     { if (type_ == FieldType::kBytes)  { delete value_.sval; value_.sval = v; }}

private:
    static const std::string kDefaultBytes;

    FieldType type_;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
        std::string* sval;
    } value_;
};

enum class CompareOp : char {
    kLess,
    kGreater,
    kEqual,
};

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op);

FieldValue* CopyValue(const FieldValue& f);
void EncodeFieldValue(std::string* buf, FieldValue* v);
void EncodeFieldValue(std::string* buf, FieldValue* v, uint32_t col_id);

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
