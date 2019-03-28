#include "storage_reader.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "logger.h"
#include "base/util.h"
#include "storage/log_file.h"
#include "server_impl.h"
#include "raft_impl.h"
#include "storage/storage_disk.h"
#include "../../../server/common_thread.h"

namespace sharkstore {
namespace raft {
namespace impl {

StorageReader::StorageReader(const uint64_t id,
                             const std::function<bool(const std::string&, errorpb::Error *&err)>& f0,
                             const std::function<bool(const metapb::RangeEpoch &meta, errorpb::Error *&err)>& f1, 
                             RaftServer* server,
                             DbInterface* db,
                             dataserver::WorkThread* trd) :
        keyInRange(f0), EpochIsEqual(f1), id_(id), server_(server), db_(db), trd_(trd)
{
    log_files_.clear();
};

StorageReader::~StorageReader() { Close(); }

//task func
Status StorageReader::Run() {
    do {
        if (!running_) break;

        {
            //std::unique_lock<std::mutex> lock(mtx_);
            if (0 == GetCommitFiles()) {
                //cond_.wait(lock);
                break;
            }
        }

        ProcessFiles();
    } while (false);

    return Status::OK();
}

size_t StorageReader::GetCommitFiles() {
    auto raft = std::static_pointer_cast<RaftImpl>( server_->FindRaft(id_) );
    assert(raft != nullptr);
    auto s =  std::static_pointer_cast<DiskStorage>( raft->GetStorage() );
    assert(s != nullptr);

    s->LoadCommitFiles(s->Applied());
    if (s->CommitFileCount() == 0) {
        return -1;
    }
    log_files_.swap(s->GetLogFiles());

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
                RAFT_LOG_ERROR("LogFile::Get() error, path: %s index: %llu", (*f)->Path().c_str(), i);
                break;
            }

            auto cmd = decodeEntry(ent);
            if (cmd) {
                auto ret = ApplyRaftCmd(*cmd);
                if (!ret.ok()) {
                    RAFT_LOG_ERROR("StorageReader::ApplyRaftCmd() error, path: %s index: %llu", (*f)->Path().c_str(), i);
                    break;
                }
                ret = StoreAppliedIndex(curr_seq_, curr_index_);
                if (!ret.ok()) {
                    RAFT_LOG_ERROR("StorageReader::ProcessFiles() warn, path: %s index: %llu", (*f)->Path().c_str(), i);
                    break;
                }
            }
        } //end one file

        if (i == (*f)->LastIndex()) {
            done_files_.emplace(std::pair<uint64_t, uint64_t>((*f)->Seq(), (*f)->LastIndex()));
            log_files_.erase(f);
        } else {
            RAFT_LOG_WARN("StorageReader::ApplyIndex() error, path: %s index: %llu", (*f)->Path().c_str(), i);
            f++;
        }
    } //end all file
    return Status::OK();
}

Status StorageReader::listLogs() {
    for (auto f : log_files_) {
        RAFT_LOG_DEBUG("path: %s \nfile_size: %llu \nlog item size: %d",
                       f->Path().c_str(), f->FileSize(), f->LogSize());
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
    dataserver::Work w;
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

    if(!tryPost(std::bind(&StorageReader::Run, shared_from_this()))) {
        RAFT_LOG_WARN("StorageReader::tryPost fail...");
    }
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

uint64_t StorageReader::Applied() {
    return applied_;
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
        case raft_cmdpb::CmdType::RawDelete:
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
        default:
            //impl::RAFT_LOG_ERROR("Apply cmd type error %s", CmdType_Name(cmd.cmd_type()).c_str());
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }
}

Status StorageReader::storeRawPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    RAFT_LOG_DEBUG("storeRawPut begin");
    auto &req = cmd.kv_raw_put_req();
    auto btime = NowMicros();
    errorpb::Error *err = nullptr;

    do {
        if (!keyInRange(req.key(), err)) {
            RAFT_LOG_WARN("storeUpdate failed, epoch is changed.error:%s", err->message().c_str());
            ret = Status(Status::kInvalidArgument, "key not in range", "");
            break; 
        }

        ret = db_->Put(req.key(), req.value());
        if (!ret.ok()) {
            RAFT_LOG_ERROR("storeRawPut failed, code:%d, msg:%s", 
                    ret.code(), ret.ToString().c_str());
            break;
        }
    } while (false);

    return ret;
}

Status StorageReader::storeRawDelete(const raft_cmdpb::Command &cmd) 
{
    Status ret;
    RAFT_LOG_DEBUG("storeRawDelete begin");
    auto &req = cmd.kv_raw_delete_req();
    auto btime = NowMicros();
    errorpb::Error *err = nullptr;

    do {
        if (!keyInRange(req.key(), err)) {
            RAFT_LOG_WARN("storeUpdate failed, epoch is changed.error:%s", err->message().c_str());
            ret = Status(Status::kInvalidArgument, "key not in range", "");
            break; 
        } 

        ret = db_->Delete(req.key());
        if (!ret.ok()) {
            RAFT_LOG_ERROR("storeRawDelete failed, code %d, msg:%s",
                    ret.code(), ret.ToString().c_str());
            break;
        }

    } while (false) ;

    return ret;
}

Status StorageReader::storeInsert(const raft_cmdpb::Command & cmd)
{
    Status ret;
//    uint64_t affected_keys = 0;
//    RAFT_LOG_DEBUG("storeInsert begin.");
//
//    auto &req = cmd.insert_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//
//    do {
//        auto &epoch = cmd.verify_epoch();
//
//        if (!EpochIsEqual(epoch, err)) {
//            RAFT_LOG_WARN("storeUpdate failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        }
//
//        ret = db_->Insert(req, &affected_keys);
//        if (!ret.ok()){
//            RAFT_LOG_ERROR("storeInsert failed, code:%d, msg:%s",
//                    ret.code(), ret.ToString().c_str());
//            break;
//        }
//
//    } while(false); 

    return ret;
}

Status StorageReader::storeUpdate(const raft_cmdpb::Command & cmd) 
{
    Status ret;
    uint64_t affected_keys = 0; 
//    uint64_t update_bytes = 0;
//    
//    RAFT_LOG_DEBUG("storeUpdate begin.");
//
//    auto &req = cmd.update_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//    
//    do {
//        auto &epoch = cmd.verify_epoch();
//
//        if (!EpochIsEqual(epoch, err)) {
//            RAFT_LOG_WARN("storeUpdate failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        }
//
//        ret = db_->update(req, &affected_keys, &update_bytes);
//        if (!ret.ok()){
//            RAFT_LOG_ERROR("storeUpdate failed, code:%d, msg:%s",
//                    ret.code(), ret.ToString().c_str());
//            break;
//        }
//    } while(false); 

    return ret;
}

Status StorageReader::storeDelete(const raft_cmdpb::Command & cmd)
{
    Status ret;
//    uint64_t affected_keys = 0;
//
//    RAFT_LOG_DEBUG("storeDelte begin.");
//
//    auto &req = cmd.delete_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//
//    do {
//        auto &key = req.key();
//
//        if (key.empty()){
//            auto &epoch = cmd.verify_epoch();
//
//            if (!EpochIsEqual(epoch, err)){ 
//                RAFT_LOG_WARN("storeDelete failed, epoch is changed.error:%s", err->message().c_str());
//                ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//                break;
//            }
//        } else {
//            if (!keyInRange(key, err)) { 
//                RAFT_LOG_WARN("storeUpdate failed, epoch is changed.error:%s", err->message().c_str());
//                ret = Status(Status::kInvalidArgument, "key not in range", "");
//                break;
//            }
//        } 
//
//        ret = db_->DeleteRow(req, &affected_keys);
//        if (!ret.ok()) {
//            RAFT_LOG_ERROR("storeDelete failed, code:%d, msg:%s",
//                    ret.code, ret.ToString().c_str());
//            break;
//        }
//    } while(false);

    return ret;
}

Status StorageReader::storeKVSet(const raft_cmdpb::Command & cmd)
{
    Status ret;
//    uint64_t affected_keys = 0;
//
//    RAFT_LOG_DEBUG("storeKvSet begin.");
//
//    auto &req = cmd.kv_set_req();
//    auto btime = NowMicros(); 
//    errorpb::Error *err = nullptr;
//
//    do {
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)){ 
//            RAFT_LOG_WARN("storeKvSet failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        }
//
//        if (req.case_() != kvrpcpb::EC_Force) {
//            bool bExists = db_->KeyExists(req.kv().key());
//            if ((req.case_() == kvrpcb::EC_Exists && !bExists) 
//                    || (req.case_() == kvrpcpb::EC_NotExists && bExists)) { 
//
//                break;
//            }
//
//            if (bExists) {
//                affected_keys = 1;
//            } 
//        }
//
//        ret = db_->Put(req.kv().key(), req.kv().value());
//        if (!ret.ok()) {
//            RAFT_LOG_ERROR("storeKvSet failed. code:%d, msg:%s",
//                    ret.code, ret.ToString().c_str());
//            break;
//        }
//    } while(false); 

    return ret;
}

Status StorageReader::storeKVBatchSet(const raft_cmdpb::Command & cmd) 
{
    Status ret;
//    uint64_t affected_keys = 0;
//
//    RAFT_LOG_DEBUG("storeKvBatchSet begin.");
//
//    auto &req = cmd.kv_batch_set_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//    auto total_size = 0, total_count = 0;
//
//    do {
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)){ 
//            RAFT_LOG_WARN("storeKvBatchSet failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        }
//
//        std::vector<std::pair<std::string, std::string>> keyValues;
//        
//        auto existCase = req.case_();
//        for (int i = 0, count = req.kvs_size(); i < count; ++i) {
//            auto kv = req.kvs(i);
//            do {
//                if (req.case_() != kvrpcpb::EC_Force) {
//                    bool bExists = db_->KeyExists(kv.key());
//                    if ((existCase == kvrpcpb::EC_Exists && !bExists) ||
//                            (existCase == kvrpcpb::EC_NotExits && bExists)) {
//
//                        break;
//                    }
//
//                    if (bExists) {
//                        ++affected_keys;
//                    }
//                }
//
//                total_size += kv.key().size() + kv.value().size();
//                ++total_count;
//
//                keyValues.push_back(std::pair<std::string, std::string>(kv.key(), kv.value()));
//
//            } while(false);
//
//            ret = db_->BatchSet(keyValues);
//
//            if (!ret.ok()) {
//                RAFT_LOG_ERROR("storeKvBatchSet failed, ret:%d, msg:%s",
//                        ret.code(), ret.ToString().c_str());
//                break;
//            }
//        }
//        
//    } while (false);

    return ret;
} 

Status StorageReader::storeKVDelete(const raft_cmdpb::Command & cmd) 
{
    Status ret;

//    RAFT_LOG_DEBUG("storeKvDelete begin.");
//
//    auto &req = cmd.kv_delete_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//
//    do {
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)){ 
//            RAFT_LOG_WARN("storeKvDelete failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        }
//
//        ret = db_->Delete(req.key());
//        if (!ret.ok()) {
//            RAFT_LOG_ERROR("storeKvDelete failed, code:%d, msg:%s", 
//                    ret.code(), ret.ToString().c_str());
//            break;
//        }
//    } while (false);

    return ret;
}

Status StorageReader::storeKVBatchDelete(const raft_cmdpb::Command & cmd)
{
    Status ret;
//    uint64_t affected_keys = 0;
//
//    RAFT_LOG_DEBUG("storeKvBatchDelete begin.");
//
//    auto &req = cmd.kv_batch_del_req();
//    auto btime = NowMicros();
//    errorpb::Error *err = nullptr;
//
//    do {
//        auto &epoch = cmd.verify_epoch();
//        if (!EpochIsEqual(epoch, err)){ 
//            RAFT_LOG_WARN("storeKvBatchDelete failed, epoch is changed.error:%s", err->message().c_str());
//            ret = Status(Status::kInvalidArgument, "epoch is changed", "");
//            break;
//        } 
//
//        std::vector<std::string> delKeys(req.keys_size());
//        for (int i = 0, count = req.keys_size(); i < count; ++i ) {
//            auto &key = req.keys(i);
//            if (req.case_() == kvrpcpb::EC_Exists 
//                    || req.case_() == kvrpcpb::EC_AnyCase) {
//
//                if (db_->KeyExists(key)) {
//                    ++affected_keys;
//                    delKeys.push_back(std::move(key));
//                } 
//            } else {
//                delKeys.push_back(std::move(key));
//            }
//        }
//
//        ret = db_->BatchDelete(delKeys);
//
//        if (!ret.ok()) {
//            RAFT_LOG_ERROR("storeKvBatchDelete failed, code:%d, msg:%s",
//                    ret.code(), ret.ToString().c_str());
//            break;
//        } 
//
//    } while(false);

    return ret;
}

Status StorageReader::storeKVRangeDelete(const raft_cmdpb::Command & cmd)
{
    Status ret; 
//    std::string last_key;
//
//    RAFT_LOG_DEBUG("storeKvRangeDelete begin.");
//
//    auto &req = cmd.kv_range_del_req();
//    auto btime = NowMicros();
//    
//    do {
//        //auto start = std::max(req.start(), start_key_);
//        ;
//    } while (false);
//
    return ret;
}


//} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
