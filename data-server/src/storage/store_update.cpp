static Status updateRow(kvrpcpb::KvPair* row, const RowResult& r);

Status Store::Update(const kvrpcpb::UpdateRequest& req, uint64_t* affected, uint64_t* update_bytes) {
    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    uint64_t count = 0;
    uint64_t all = 0;
    uint64_t limit = req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;

    auto batch = db_->NewBatch();
    uint64_t bytes_written = 0;

    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            ++all;
            if (all > offset) {
                kvrpcpb::KvPair kv;
                Status s;

                s = updateRow(&kv, *r);
                if (!s.ok()) {
                    return s;
                }

                batch->Put(kv.key(), kv.value());
                ++(*affected);
                bytes_written += kv.key().size() + kv.value().size();

                if (++count >= limit) break;
            }
        }
    }

    if (s.ok()) {
        auto rs = db_->Write(batch.get());
        if (!rs.ok()) {
            s = Status(Status::kIOError, "update batch write", rs.ToString());
        }
    }

    *update_bytes = bytes_written;
    return s;
}

static Status updateRow(kvrpcpb::KvPair* row, const RowResult& r) {
    std::string final_encode_value;
    const auto& origin_encode_value = r.Value();

    for (auto it = r.FieldValueList().begin(); it != r.FieldValueList().end(); it++) {
        auto& field = *it;

        std::string value;
        auto it_field_update = r.UpdateFieldMap().find(field.column_id_);
        if (it_field_update == r.UpdateFieldMap().end()) {
            value.assign(origin_encode_value, field.offset_, field.length_);
            final_encode_value.append(value);
            continue;
        }

        // 更新列值
        // delta field value
        auto it_value_delta = r.UpdateFieldDeltaMap().find(field.column_id_);
        if (it_value_delta == r.UpdateFieldDeltaMap().end()) {
            return Status(Status::kUnknown, std::string("no such update column id: " + field.column_id_), "");
        }
        FieldValue* value_delta = it_value_delta->second;

        // orig field value
        FieldValue* value_orig = r.GetField(field.column_id_);
        if (value_orig == nullptr) {
            return Status(Status::kUnknown, std::string("no such column id " + field.column_id_), "");
        }

        // kv rpc field
        kvrpcpb::Field* field_delta = it_field_update->second;

        switch (field_delta->field_type()) {
            case kvrpcpb::Assign:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Plus:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() + value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() + value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() + value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Minus:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() - value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() - value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() - value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Mult:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() * value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() * value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() * value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Div:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        if (value_delta->Int() != 0) {
                            value_orig->AssignInt(value_orig->Int() / value_delta->Int());
                        }
                        break;
                    case FieldType::kUInt:
                        if (value_delta->UInt() != 0) {
                            value_orig->AssignUint(value_orig->UInt() / value_delta->UInt());
                        }
                        break;
                    case FieldType::kFloat:
                        if (value_delta->Float() != 0) {
                            value_orig->AssignFloat(value_orig->Float() / value_delta->Float());
                        }
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            default:
                return Status(Status::kUnknown, "unknown field operator type", "");
        }

        // 重新编码修改后的field value
        EncodeFieldValue(&value, value_orig, field.column_id_);
        final_encode_value.append(value);
    }

    row->set_key(r.Key());
    row->set_value(final_encode_value);

    return Status::OK();
}
