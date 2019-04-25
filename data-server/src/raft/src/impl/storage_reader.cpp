#include "storage_reader.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>
#include <string>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "logger.h"
#include "base/util.h"
#include "storage/log_file.h"
#include "server_impl.h"
#include "raft_impl.h"
#include "storage/storage_disk.h"

namespace sharkstore {
namespace raft {
namespace impl {

static const int PERSIST_DONE_FILE_SIZE = 10;

StorageReader::StorageReader(const uint64_t id, const uint64_t index, RaftServer* server) :
        id_(id), start_index_(index), server_(server) {}

StorageReader::~StorageReader() { Close(); }

Status StorageReader::getCurrLogFile(const uint64_t idx) {
    bool reload{false};
    do {
        if (log_files_.empty() && curr_log_file_ == nullptr) {
            reload = true;
        } else {
            uint64_t min{0}, max{0};
            if (curr_log_file_ != nullptr) {
                min = curr_log_file_->Index();
                max = curr_log_file_->LastIndex();
                if (idx >= min && idx <= max) break;
            }

            if (!log_files_.empty()) {
                min = log_files_.front()->Index();
                max = log_files_.back()->LastIndex();
                if (idx >= min && idx <= max) {
                    curr_log_file_ = nullptr;
                    break;
                } 
            }
            reload = true;
        }
    } while (false);

    if (reload) {
        while (!log_files_.empty()) {
            log_files_.pop();
        }
        curr_log_file_.reset();

//        auto raft = std::static_pointer_cast<RaftImpl>( server_->FindRaft(id_) );
//        assert(raft != nullptr);
//        auto s =  std::static_pointer_cast<DiskStorage>( raft->GetStorage() );
//       assert(s != nullptr);

        auto raft = server_->FindRaft(id_);
        if ( raft != nullptr ) {
            auto s = std::static_pointer_cast<DiskStorage>(raft->GetStorage());
            if (s != nullptr )  {
                auto r = s->LoadCommitFiles(idx);
                if (!r.ok()) {
                    RAFT_LOG_ERROR("LoadCommitFiles error: %s, reader appley_id:%" PRIu64 , r.ToString().c_str(), idx);
                    return r;
                } 
                for (auto &f : s->GetCommitFiles()) {
                    log_files_.emplace(f);
                }

            } else { 
                RAFT_LOG_ERROR("getCurrLogFile Cant't GetStoree from raft idx:%" PRIu64, idx);
                return Status(Status::kNotFound, "Can't Found raft id:", std::to_string(id_));
            } 
        } else { 
            RAFT_LOG_ERROR("getCurrLogFile Cant't Found raft id:%" PRIu64, idx);
            return Status(Status::kNotFound, "Can't Found raft id:", std::to_string(id_));
        } 
    }

    if (log_files_.empty() && curr_log_file_ == nullptr) {
        return Status(Status::kNotFound, "getCurrLogFile:there's not a complete raft log file", "");
    }
    return Status::OK();
}

Status StorageReader::GetData(const uint64_t idx, std::shared_ptr<raft_cmdpb::Command>& cmd) {

    RAFT_LOG_INFO("GetData , start_index:%" PRIu64  "applied_:%" PRIu64 "read Idx:%" PRIu64, start_index_, applied_, idx);
    if ((start_index_ > 0 && idx < start_index_) || 
            (applied_ > 0 && idx > applied_ + 1)) 
    {
        return Status(Status::kOutOfBound, "StorageReader::GetData error", "passin invalid index");
    } 

    Status r;
    r = getCurrLogFile(idx);
    if (!r.ok()) {
        RAFT_LOG_ERROR("getCurrLogFile error, %s, reader apply log index:%" PRIu64 , r.ToString().c_str(), idx);
        return r;
    }
    listLogs();

    EntryPtr ent = nullptr;
    //judge index scope
    if (curr_log_file_ == nullptr && !log_files_.empty()) {
        curr_log_file_ = log_files_.front();
        log_files_.pop();
    }

    if (curr_log_file_ == nullptr) {
        return Status(Status::kNotFound, "GetData", "log file isn't ready");
    }
    r = curr_log_file_->Get(idx, &ent);
    if (!r.ok()) {
        return r;
    }

    r = decodeEntry(ent, cmd);
    if (!r.ok()) {
        return r;
    }

    appliedTo(idx);
    return Status::OK();
}

Status StorageReader::listLogs() {
    if (log_files_.size() > 0) {
        auto f = log_files_.front();
        RAFT_LOG_DEBUG("files: %zd >>>1.)path: %s \nfile_size: %" PRIu64 " \nlog item size: %d",
                log_files_.size(), f->Path().c_str(), f->FileSize(), f->LogSize());
        f = log_files_.back();
        RAFT_LOG_DEBUG("files: %zd >>>1.)path: %s \nfile_size: %" PRIu64 " \nlog item size: %d",
                log_files_.size(), f->Path().c_str(), f->FileSize(), f->LogSize());
    }
    return Status::OK();
}

Status StorageReader::Close() {
    while (!log_files_.empty()) {
        log_files_.pop();
    }
    return Status::OK();
}

Status StorageReader::decodeEntry(EntryPtr entry, std::shared_ptr<raft_cmdpb::Command>& raft_cmd) {
    raft_cmd = std::make_shared<raft_cmdpb::Command>();
    auto data =  entry->data().data();
    auto len = static_cast<int>(entry->data().size());
    
    if (len > 0 && entry->type() == pb::ENTRY_NORMAL ) { 
        raft_cmd->ParseFromArray(data, len);
    }
    
    return Status::OK(); 
}


} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
