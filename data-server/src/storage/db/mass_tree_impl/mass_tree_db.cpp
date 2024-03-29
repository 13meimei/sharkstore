#include "mass_tree_db.h"

#include <mutex>
#include "masstree-beta/masstree_insert.hh"
#include "masstree-beta/masstree_remove.hh"
#include "masstree-beta/masstree_scan.hh"
#include "masstree-beta/masstree_stats.hh"
#include "masstree-beta/json.hh"

#include "scaner.h"

volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;

namespace sharkstore {
namespace dataserver {
namespace storage {

// 保护 thread_info list
static std::mutex thread_infos_mu;

static threadinfo* createThreadInfo() {
    std::lock_guard<std::mutex> lock(thread_infos_mu);
    return threadinfo::make(threadinfo::TI_PROCESS, -1);
}

thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> MassTreeDB::thread_info_(createThreadInfo());

MassTreeDB::MassTreeDB() : tree_(new TreeType) {
    tree_->initialize(*thread_info_);
}

Status MassTreeDB::Put(const std::string& key, const std::string& value) {
    if (key.size() > MASSTREE_MAXKEYLEN) {
        return Status(Status::kInvalidArgument, "key size too large", std::to_string(key.size()));
    }

    Masstree::Str tree_key(key);
    thread_info_->rcu_start();
    TreeType::cursor_type lp(*tree_, tree_key);
    if (lp.find_insert(*thread_info_)) {
        delete lp.value();
    }
    lp.value() = new std::string(value);
    lp.finish(1, *thread_info_);
    thread_info_->rcu_stop();

    return Status::OK();
}

Status MassTreeDB::Get(const std::string& key, std::string* value) {
    Masstree::Str tree_key(key);
    std::string *tree_value = nullptr;

    thread_info_->rcu_start();
    if (tree_->get(tree_key, tree_value, *thread_info_)) {
        if (tree_value != nullptr) {
            value->assign(*tree_value);
        }
        thread_info_->rcu_stop();
        return Status::OK();
    } else {
        thread_info_->rcu_stop();
        return Status(Status::kNotFound);
    }
}

Status MassTreeDB::Delete(const std::string& key) {
    Masstree::Str tree_key(key);

    thread_info_->rcu_start();
    TreeType::cursor_type lp(*tree_, tree_key);
    if (lp.find_locked(*thread_info_)) {
        delete lp.value();
    }
    lp.finish(-1, *thread_info_);
    thread_info_->rcu_stop();

    return Status::OK();
}

void MassTreeDB::EpochIncr() {
    globalepoch += 1;
    auto& ti = *thread_info_; // 确保thread_local先初始化，避免死锁
    {
        std::lock_guard<std::mutex> lock(thread_infos_mu);
        active_epoch = ti.min_active_epoch();
    }
}

void MassTreeDB::RCUFree() {
    // 尝试回收
    thread_info_->rcu_start();
    thread_info_->rcu_stop();
}

uint64_t MassTreeDB::GetCounter(threadcounter c) {
    uint64_t count = 0;
    std::lock_guard<std::mutex> lock(thread_infos_mu);
    for (threadinfo* ti = threadinfo::allthreads; ti; ti = ti->next()) {
        prefetch((const void*) ti->next());
        count += ti->counter(c);
    }
    return count;
}

template int MassTreeDB::Scan(const std::string&, Scaner&);

template <typename F>
int MassTreeDB::Scan(const std::string& begin, F& scanner) {
    thread_info_->rcu_start();
    auto count = tree_->scan(begin, true, scanner, *thread_info_);
    thread_info_->rcu_stop();
    return count;
}

std::unique_ptr<Scaner> MassTreeDB::NewScaner(const std::string& start, const std::string& limit, size_t max_per_scan) {
    std::unique_ptr<Scaner> ptr(new Scaner(this, start, limit, max_per_scan));
    return ptr;
}

std::string MassTreeDB::Stat() {
    auto j = Masstree::json_stats(*tree_, *thread_info_);
    if (j) {
        return j.unparse(lcdf::Json::indent_depth(1).tab_width(2).newline_terminator(true));
    } else {
        return "";
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
