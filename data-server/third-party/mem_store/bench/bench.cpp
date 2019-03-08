//
// Created by young on 19-2-28.
//
#include <pthread.h>
#include <thread>
#include <assert.h>
#include <sys/time.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <strings.h>
#include <csignal>

#include <gperftools/profiler.h>
#include "mem_store/mem_store.h"

using namespace memstore;

class StoreBench: public Store<std::string> {
public:
    StoreBench() = delete;
    explicit StoreBench(uint64_t op_type,
                        uint64_t data_number_per_thread = 1,
                        uint64_t thread_number = 1):
            op_type_((static_cast<OpType>(op_type))),
            thread_number_(thread_number),
            data_number_per_thread_(data_number_per_thread) {
        count_.store(0, std::memory_order_relaxed);
        time_.store(0, std::memory_order_relaxed);
        printer_ = std::move(std::thread ([this]() {
            while (true) {
                auto lk = std::unique_lock<std::mutex>(lock_);
                auto status = cond_.wait_for(lk, std::chrono::milliseconds(300));
                if (status == std::cv_status::timeout) {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                    std::cout << count_ << std::endl;
                } else {
                    lk.unlock();
                    return;
                }
            }
        }));
    }

    ~StoreBench() {
        std::unique_lock<std::mutex> lk(lock_);
        // todo notify maybe not effected when printer thread is not doing wait_for
        cond_.notify_one();
        lk.unlock();
        printer_.detach();
    }

public:
    void Run() {
        std::cout << "----\nthreads: "  << thread_number_ <<
                  " numbers: " << data_number_per_thread_ << std::endl;

        ProfilerStart("./auto.prof");
        for (auto i = uint(0); i < thread_number_; i++) {
            auto t = std::move(std::thread([this]() {
                switch (op_type_) {
                    case put:
                        putBench();
                        break;
                    case get:
                        getBench();
                        break;
                    case del:
                        delBench();
                        break;
                }
            }));

            thread_vec_.push_back(std::move(t));
        }

        for (auto it = thread_vec_.begin(); it != thread_vec_.end(); it++) {
            it->join();
        }
        ProfilerStop();

        std::cout << "count: " << count_ << std::endl;
        std::cout << "chrono clock(s): " << time_ << std::endl;

        if (time_ < 1) {
            std::cout << "ops: " << count_ << std::endl;
        } else {
            std::cout << "ops: " << count_/time_ << std::endl;
        }
    }

private:
    void putBench() {
        auto tid = std::this_thread::get_id();

        auto t0 = std::chrono::system_clock::now();

        char buf[32] = {0};
        for (auto i = uint64_t(0); i < data_number_per_thread_; i++) {
            sprintf(buf, "%lu-%lu", tid, i);
            auto res = store_.Put(std::string(buf), std::string(buf));
            assert(res == 0);

            count_.fetch_add(1, std::memory_order_relaxed);
        }

        auto t1 = std::chrono::system_clock::now();
        auto time = std::chrono::duration_cast<std::chrono::seconds>(t1-t0).count();

//        count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
        time_.fetch_add(time, std::memory_order_relaxed);
    }

    void getBench() {
        auto tid = std::this_thread::get_id();

        // put
        char buf[32] = {0};
        for (auto i = uint64_t(0); i < data_number_per_thread_; i++) {
            sprintf(buf, "%lu-%lu", tid, i);
            auto res = store_.Put(std::string(buf), std::string(buf));
            assert(res == 0);
        }

        // get
        auto t0 = std::chrono::system_clock::now();

        for (auto i = uint64_t(0); i < data_number_per_thread_; i++) {
            sprintf(buf, "%lu-%lu", tid, i);
            std::string val;
            auto res = store_.Get(std::string(buf), &val);
//            assert(res == 0);
//            assert(val == std::string(buf));

            count_.fetch_add(1, std::memory_order_relaxed);
        }

        auto t1 = std::chrono::system_clock::now();
        auto time = std::chrono::duration_cast<std::chrono::seconds>(t1-t0).count();

//        count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
        time_.fetch_add(time, std::memory_order_relaxed);
    }

    void delBench() {
        auto tid = std::this_thread::get_id();

        // put
        char buf[32] = {0};
        for (auto i = uint64_t(0); i < data_number_per_thread_; i++) {
            sprintf(buf, "%lu-%lu", tid, i);
            auto res = store_.Put(std::string(buf), std::string(buf));
            assert(res == 0);
        }

        // del
        auto t0 = std::chrono::system_clock::now();

        for (auto i = uint64_t(0); i < data_number_per_thread_; i++) {
            sprintf(buf, "%lu-%lu", tid, i);
            store_.Delete(std::string(buf));

//            std::string val;
//            auto res = store_.Get(std::string(buf), &val);
//            assert(res != 0);

            count_.fetch_add(1, std::memory_order_relaxed);
        }

        auto t1 = std::chrono::system_clock::now();
        auto time = std::chrono::duration_cast<std::chrono::seconds>(t1-t0).count();

//        count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
        time_.fetch_add(time, std::memory_order_relaxed);
    }

private:
    enum OpType {
        put,
        get,
        del,
    } op_type_ = get;
    uint64_t thread_number_ = 1;
    uint64_t data_number_per_thread_ = 1;

    Store<std::string> store_;
    std::vector<std::thread> thread_vec_;

    std::atomic<uint64_t >count_;
    std::atomic<uint64_t > time_;

    std::thread printer_;

    std::mutex lock_;
    std::condition_variable cond_;
};

static bool profiler_switch = false;
static void signal_handler(int signal) {
    profiler_switch = !profiler_switch;

    if (profiler_switch) {
        ProfilerStart("./manual.prof");
    } else {
        ProfilerStop();
    }
}


int main(int argc, char** argv) {
    int opt;
    auto op_type = 0;
    auto thread_number = 0;
    auto data_number_per_thread = 0;
    if (argc != 1+3*2) {
        std::cout << "usage: " << argv[0] << " type PUT|GET|DEL threads C data N" << std::endl;
        exit(-1);
    }
    for (auto i = 1; i < 3*2; i++) {
        if (!strcasecmp(argv[i], "type")) {
            auto v = argv[++i];
            std::cout << "type: " << v << std::endl;
            if (!strcasecmp(v, "put")) {
                op_type = 0;
            } else if (!strcasecmp(v, "get")) {
                op_type = 1;
            } else if (!strcasecmp(v, "del")) {
                op_type = 2;
            } else {
                std::cout << "unknow op type!" << std::endl;
                exit(-2);
            }
        } else if (!strcasecmp(argv[i], "threads")) {
            thread_number = atoi(argv[++i]);
            std::cout << "threads: " << thread_number << std::endl;
        } else if (!strcasecmp(argv[i], "data")) {
            data_number_per_thread = atoi(argv[++i]);
            std::cout << "data: " << data_number_per_thread << std::endl;
        }
    }

    std::signal(SIGUSR1, signal_handler);

    StoreBench b(op_type, data_number_per_thread, thread_number);
    b.Run();

    return 0;
}

