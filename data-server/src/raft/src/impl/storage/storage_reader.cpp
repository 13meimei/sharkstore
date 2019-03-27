#include "raft/storage_reader.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "../logger.h"
#include "base/util.h"
#include "log_file.h"
#include "../server_impl.h"
#include "../raft_impl.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

StorageReader::StorageReader(const uint64_t id,
                             std::function<bool(const std::string&)> f0,
                             std::function<bool(const metapb::Range &meta)> f1,
                             RaftServerImpl *server,
                             DbInterface* db,
                             sharkstore::raft::impl::WorkThread* trd) :
        keyInRange(f0), EpochIsEqual(f1), id_(id), server_(server), db_(db), trd_(trd)
{
    log_files_.clear();
};

StorageReader::~StorageReader() { Close(); }

Status StorageReader::Run() {
    do {
        if (!running_) break;

        {
            std::unique_lock<std::mutex> lock(mtx_);
            if (0 == GetCommitFiles()) {
                cond_.wait(lock);
            }
        }

        ProcessFiles();
    } while (true);

    return Status::OK();
}
size_t StorageReader::GetCommitFiles() {
    auto raft = std::static_pointer_cast<RaftImpl>( server_->FindRaft(id_) );
    assert(raft != nullptr);
    storage_ =  std::static_pointer_cast<DiskStorage>( raft->GetStorage() );
    assert(storage_ != nullptr);

    storage_->LoadCommitFiles(storage_->Applied());
    if (storage_->CommitFileCount() == 0) {
        return -1;
    }
    log_files_.swap(storage_->GetLogFiles());

    for (auto f = log_files_.begin(); f != log_files_.end(); ) {
        if (done_files_.find((*f)->Seq()) != done_files_.end()) {
            log_files_.erase(f);
        } else {
            f++;
        }
    }

    if (log_files_.size() == 0) {
        RAFT_LOG_INFO("GetCommitFiles() all file processed, wait...");
    }

#ifndef NDEBUG
    listLogs();
#endif
    return log_files_.size();
}

//TO DO iterator files and decode
Status StorageReader::ProcessFiles() {
    Status ret;
    EntryPtr ent = nullptr;

    uint64_t i = 0;
    for (auto f = log_files_.begin(); f != log_files_.end(); ) {
        for (i = (*f)->Index(); i<(*f)->LastIndex(); i++) {
            if (i == (*f)->Index()) {
                curr_seq_ = (*f)->Seq();
                curr_index_ = (*f)->Index();
            }

            ret = (*f)->Get(i, &ent);
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
                ret = StoreAppliedIndex(curr_seq_, curr_index_);
                //ret = AppliedTo(i);
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

Status StorageReader::StoreAppliedIndex(const uint64_t& seq, const uint64_t& index) {
    std::string key = sharkstore::dataserver::storage::kPersistRaftLogPrefix + std::to_string(id_);
    const std::string& value = std::to_string(index);
    // put into db
    auto ret = db_->Put(key, value);
    if (!ret.ok()) {
        return Status(Status::kIOError, "put", ret.ToString());
    }

    AppliedTo(index);
    return Status::OK();
}

/*Status StorageReader::ApplySnapshot(const pb::SnapshotMeta& meta) {
    return Status::OK();
}*/

bool StorageReader::tryPost(const std::function<void()>& f) {
    Work w;
    w.owner = id_;
    w.stopped = &running_;
    w.f0 = f;
    return trd_->tryPost(w);
}

Status StorageReader::Notify(const uint64_t range_id, const uint64_t index) {
    if (!running_) {
        return Status(Status::kShutdownInProgress, "server is stopping",
                      std::to_string(id_));
    }

    tryPost(std::bind(&StorageReader::Run, shared_from_this()));
    return Status::OK();
}

Status StorageReader::AppliedTo(uint64_t applied) {
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
    running_ = false;
    return Status::OK();
}

Status StorageReader::saveApplyIndex(uint64_t range_id, uint64_t apply_index) {
    std::string key = sharkstore::dataserver::storage::kPersistRaftLogPrefix + std::to_string(range_id);
    //TO DO rocksdb
    //auto ret = db_->Put(rocksdb::WriteOptions(), key, std::to_string(apply_index));
    Status ret;
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta save apply", ret.ToString());
    }
}

std::shared_ptr<raft_cmdpb::Command> StorageReader::decodeEntry(EntryPtr entry) {
    std::shared_ptr<raft_cmdpb::Command> raft_cmd = std::make_shared<raft_cmdpb::Command>();
    google::protobuf::io::ArrayInputStream input(entry->data().c_str(), static_cast<int>(entry->ByteSizeLong()));
    if(!raft_cmd->ParseFromZeroCopyStream(&input)) {
        RAFT_LOG_ERROR("parse raft command failed");
        //return Status(Status::kCorruption, "parse raft command", EncodeToHex(entry->data()));
        return nullptr;
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
