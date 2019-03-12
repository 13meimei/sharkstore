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

class StoreBench {
public:
    StoreBench() = delete;
    explicit StoreBench(uint64_t op_type,
                        uint64_t data_number_per_thread = 1,
                        uint64_t thread_number = 1):
            op_type_((static_cast<OpType>(op_type))),
            thread_number_(thread_number),
            data_number_per_thread_(data_number_per_thread) {
        put_count_.store(0, std::memory_order_relaxed);
        put_time_.store(1, std::memory_order_relaxed);
        get_count_.store(0, std::memory_order_relaxed);
        get_time_.store(1, std::memory_order_relaxed);
        del_count_.store(0, std::memory_order_relaxed);
        del_time_.store(1, std::memory_order_relaxed);
        printer_ = std::move(std::thread ([this]() {
            return; // comment below
            while (true) {
                auto lk = std::unique_lock<std::mutex>(lock_);
                auto status = cond_.wait_for(lk, std::chrono::milliseconds(300));
                if (status == std::cv_status::timeout) {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                    // todo print
                } else {
                    lk.unlock();
                    return;
                }
            }
        }));
    }

    ~StoreBench() {
        std::unique_lock<std::mutex> lk(lock_);
        cond_.notify_one();
        lk.unlock();
        printer_.detach();
    }

public:
    void Run() {
        std::cout << "----\nthreads: "  << thread_number_ <<
                  " numbers: " << data_number_per_thread_ << std::endl;

        ProfilerStart("./auto.prof");
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
        ProfilerStop();

        std::cout << "put count: " << put_count_ << std::endl;
        std::cout << "put time(ms): " << put_time_ << std::endl;
        std::cout << "put ops: " << (put_count_*1000)/put_time_ << std::endl;

        std::cout << "get count: " << get_count_ << std::endl;
        std::cout << "get time(ms): " << get_time_ << std::endl;
        std::cout << "get ops: " << (get_count_*1000)/get_time_ << std::endl;

        std::cout << "del count: " << del_count_ << std::endl;
        std::cout << "del time(ms): " << del_time_ << std::endl;
        std::cout << "del ops: " << (del_count_*1000)/del_time_ << std::endl;

    }

private:
    void putBench() {
        std::vector<std::thread> vec;

        auto t0 = std::chrono::system_clock::now();
        for (auto n = 0; n < thread_number_; n++) {
            std::thread t([=] {
                char buf[32] = {0};
                for (auto i = 0; i < data_number_per_thread_; i++) {
                    sprintf(buf, "%lu-%lu", n, i);
                    auto res = store_.Put(std::string(buf), std::string(buf));
                    assert(res == 0);
                }
                put_count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
            });
            vec.push_back(std::move(t));
        }
        for (auto& t: vec) {
            t.join();
        }
        auto t1 = std::chrono::system_clock::now();

        auto time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
        put_time_.fetch_add(time, std::memory_order_relaxed);
    }

    void getBench() {
        std::vector<std::thread> vec;

        //////// put /////////
        {
            auto t0 = std::chrono::system_clock::now();
            for (auto n = 0; n < thread_number_; n++) {
                std::thread t([=] {
                    char buf[32] = {0};
                    for (auto i = 0; i < data_number_per_thread_; i++) {
                        sprintf(buf, "%lu-%lu", n, i);
                        auto res = store_.Put(std::string(buf), std::string(buf));
                        assert(res == 0);
                    }
                    put_count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
                });
                vec.push_back(std::move(t));
            }
            for (auto &t: vec) {
                t.join();
            }
            auto t1 = std::chrono::system_clock::now();

            auto time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            put_time_.fetch_add(time, std::memory_order_relaxed);
        }

        vec.clear();
        //////// get /////////
        {
            auto t0 = std::chrono::system_clock::now();
            for (auto n = 0; n < thread_number_; n++) {
                std::thread t([=] {
                    char buf[32] = {0};
                    for (auto i = 0; i < data_number_per_thread_; i++) {
                        sprintf(buf, "%lu-%lu", n, i);
                        std::string val;
                        auto res = store_.Get(std::string(buf), &val);
                    }
                    get_count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
                });
                vec.push_back(std::move(t));
            }
            for (auto &t: vec) {
                t.join();
            }
            auto t1 = std::chrono::system_clock::now();
            auto time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            get_time_.fetch_add(time, std::memory_order_relaxed);
        }
    }

    void delBench() {
        std::vector<std::thread> vec;

        //////// put /////////
        {
            auto t0 = std::chrono::system_clock::now();
            for (auto n = 0; n < thread_number_; n++) {
                std::thread t([=] {
                    char buf[32] = {0};
                    for (auto i = 0; i < data_number_per_thread_; i++) {
                        sprintf(buf, "%lu-%lu", n, i);
                        auto res = store_.Put(std::string(buf), std::string(buf));
                        assert(res == 0);
                    }
                    put_count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
                });
                vec.push_back(std::move(t));
            }
            for (auto &t: vec) {
                t.join();
            }
            auto t1 = std::chrono::system_clock::now();

            auto time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            put_time_.fetch_add(time, std::memory_order_relaxed);
        }

        vec.clear();
        //////// del /////////
        {
            auto t0 = std::chrono::system_clock::now();
            for (auto n = 0; n < thread_number_; n++) {
                std::thread t([=] {
                    char buf[32] = {0};
                    for (auto i = 0; i < data_number_per_thread_; i++) {
                        sprintf(buf, "%lu-%lu", n, i);
                        store_.Delete(std::string(buf));
                    }
                    get_count_.fetch_add(data_number_per_thread_, std::memory_order_relaxed);
                });
                vec.push_back(std::move(t));
            }
            for (auto &t: vec) {
                t.join();
            }
            auto t1 = std::chrono::system_clock::now();

            auto time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            get_time_.fetch_add(time, std::memory_order_relaxed);
        }
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

    std::atomic<uint64_t > put_count_;
    std::atomic<uint64_t > put_time_;
    std::atomic<uint64_t > get_count_;
    std::atomic<uint64_t > get_time_;
    std::atomic<uint64_t > del_count_;
    std::atomic<uint64_t > del_time_;

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

