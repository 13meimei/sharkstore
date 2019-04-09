#include "mass_tree_db.h"

#include "masstree-beta/masstree_insert.hh"
#include "masstree-beta/masstree_remove.hh"

volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;

namespace sharkstore {
namespace dataserver {
namespace storage {

thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> MassTreeDB::thread_info_(
        threadinfo::make(threadinfo::TI_PROCESS, -1));

MassTreeDB::MassTreeDB() : tree_(new TreeType){
    tree_->initialize(*thread_info_);
}

Status MassTreeDB::Put(const std::string& key, const std::string& value) {
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
    active_epoch = thread_info_->min_active_epoch();
}

std::unique_ptr<Scaner> MassTreeDB::NewScaner(const std::string& start, const std::string& limit, size_t max_per_scan) {
    thread_info_->rcu_start();
    std::unique_ptr<Scaner> ptr(new Scaner(tree_, start, limit, max_per_scan, []{ thread_info_->rcu_stop(); }));
    return ptr;
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
