#include "storage_reader.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>

#include "../logger.h"
#include "base/util.h"
#include "log_file.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

// 只截断已应用的减去kKeepCountBeforeApplied之前的日志
static const unsigned kKeepLogCountBeforeApplied = 30;

StorageReader::StorageReader(const uint64_t id, raft::impl::RaftServerImpl *server,
      const std::string& path, dataserver::storage::DbInterface* db) :
        id_(id), server_(server), path_(path), db_(db)
{
    log_files_.clear();
};

StorageReader::~StorageReader() { Close(); }

Status StorageReader::GetCommitFiles() {
    auto raft = server_->FindRaft(id_);
    storage_ = raft->GetStorage();
    if (storage->CommitFileCount() == 0) {
        return Status(Status::kNotFound, "GetCommitFiles() get nothing.", "commited file not exists.");
    }
    log_files_ = storage->GetLogFiles();

    for (auto f = log_files_.begin(); f != log_files_.end(); ) {
        if (done_files_.find(((*f)->Seq()) != done_files_.end())) {
            log_files_.erase(f);
        } else {
            f++;
        }
    }
    done_files_
#ifndef NDEBUG
    listLogs();
#endif
    return Status::OK();
}

//TO DO iterator files and decode
Status StorageReader::ProcessFiles() {
    Status ret;
    EntryPtr ent = nullptr;

    uint64_t i = 0;
    for (auto f = log_files_.begin(); f != log_files_.end(); ) {
        for (i = (*f)->Index(); i<(*f)->LastIndex(); i++) {
            ret = (*f)->Get(i, ent);
            if (!ret.ok()) {
                RAFT_LOG_ERROR("LogFile::Get() error, path: %s index: %llu", (*f)->Path(), i);
                break;
            }

            auto cmd = decodeEntry(ent);
            if (cmd) {
                auto ret = ApplyRaftCmd(*cmd);
                if (!ret.ok()) {
                    RAFT_LOG_ERROR("StorageReader::ApplyRaftCmd() error, path: %s index: %llu", (*f)->Path(), i);
                    break;
                }
                ret = ApplyIndex(i);
                if (!ret.ok()) {
                    RAFT_LOG_ERROR("StorageReader::ProcessFiles() warn, path: %s index: %llu", (*f)->Path(), i);
                    break;
                }
            }
        } //end one file

        if (i == (*f)->LastIndex()) {
            done_files_.emplace(std::pair<uint64_t, uint64_t>((*f)->Seq(), (*f)->LastIndex()));
            log_files_.erase(f);
        } else {
            RAFT_LOG_WARN("StorageReader::ApplyIndex() error, path: %s index: %llu", (*f)->Path(), i);
            f++;
        }
    } //end all file
    return Status::OK();
}

Status StorageReader::listLogs() {
    for (auto f : log_files_) {
        RAFT_LOG_DEBUG("path: %s \nfile_size: %llu \nlog item size: %d",
                       f->Path(), f->FileSize(), f->LogSize());
    }
    return Status::OK();
}

//StoreAppliedIndex
Status StorageReader::StoreAppliedIndex(const uint64_t& seq, const uint64_t& index) {
    std::string key = dataserver::storage::kPersistRaftLogPrefix + std::to_string(seq);
    const std::string& value = std::to_string(index);
    // put into db
    auto ret = db_->Put(key, value);
    if (!ret.ok()) {
        return Status(Status::kIOError, "put", ret.ToString());
    }

    ApplyIndex(index);
    return Status::OK();
}

Status StorageReader::ApplySnapshot(const pb::SnapshotMeta& meta) {
    return Status::OK();
}

Status StorageReader::ApplyIndex(uint64_t applied) {
    auto s = saveApplyIndex(id_, applied);
    if (!s.ok()) {
        return s;
    }
    if (applied > applied_) {
        applied_ = applied;
    }

    return s;
}

Status StorageReader::Close() {
    return Status::OK();
}

Status StorageReader::saveApplyIndex(uint64_t range_id, uint64_t apply_index) {
    std::string key = sharkstore::dataserver::storage::kPersistRaftLogPrefix + std::to_string(range_id);
    auto ret =
            db_->Put(rocksdb::WriteOptions(), key, std::to_string(apply_index));
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta save apply", ret.ToString());
    }
}

std::unique_ptr<raft_cmdpb::Command> StorageReader::decodeEntry(EntryPtr entry) {
    std::unique_ptr<raft_cmdpb::Command> raft_cmd = std::make_unique<raft_cmdpb::Command>();
    google::protobuf::io::ArrayInputStream input(entry->data(), static_cast<int>(entry->ByteSizeLong()));
    if(!raft_cmd->ParseFromZeroCopyStream(&input)) {
        RAFT_LOG_ERROR("parse raft command failed");
        return Status(Status::kCorruption, "parse raft command", EncodeToHex(entry->data()));
    }
    return raft_cmd;
}

Status StorageReader::ApplyRaftCmd(const raft_cmdpb::Command& cmd) {
    switch (cmd.cmd_type()) {
        case raft_cmdpb::CmdType::RawPut:
            return storeRawPut(cmd);
            /*case raft_cmdpb::CmdType::RawDelete:
                return storeRawDelete(cmd);
            case raft_cmdpb::CmdType::Insert:
                return storeInsert(cmd);
            case raft_cmdpb::CmdType::Update:
                return storeUpdate(cmd);
            case raft_cmdpb::CmdType::Delete:
                return storeDelete(cmd);
            case raft_cmdpb::CmdType::KvSet:
                return storeKVSet(cmd);
            case raft_cmdpb::CmdType::KvBatchSet:
                return storeKVBatchSet(cmd);
            case raft_cmdpb::CmdType::KvDelete:
                return storeKVDelete(cmd);
            case raft_cmdpb::CmdType::KvBatchDel:
                return storeKVBatchDelete(cmd);
            */
        default:
            RAFT_LOG_ERROR("Apply cmd type error %s", CmdType_Name(cmd.cmd_type()).c_str());
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }
}

Status StorageReader::storeRawPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    RAFT_LOG_DEBUG("storeRawPut begin");
    auto &req = cmd.kv_raw_put_req();
    auto btime = NowMicros();

    do {
        ret = db_->Put(req.key(), req.value());
        if (!ret.ok()) {
            RAFT_LOG_ERROR("storeRawPut failed, code:%d, msg:%s", ret.code(),
                            ret.ToString().c_str());
            break;
        }
    } while (false);

    return ret;
}


} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
