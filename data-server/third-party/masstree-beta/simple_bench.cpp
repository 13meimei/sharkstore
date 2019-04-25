#include <unistd.h>
#include <thread>
#include <vector>

#include "masstree_tcursor.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree.hh"
#include "str.hh"
#include "kvthread.hh"

volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;

class StringValuePrinter {
public:
    static void print(std::string *value, FILE* f, const char* prefix,
                      int indent, Masstree::Str key, kvtimestamp_t,
                      char* suffix) {
        fprintf(f, "%s%*s%.*s = %s%s\n",
                prefix, indent, "", key.len, key.s, (value ? value->c_str() : ""), suffix);
    }
};

struct default_query_table_params : public Masstree::nodeparams<15, 15> {
    typedef std::string* value_type;
    typedef StringValuePrinter value_print_type;
    typedef ::threadinfo threadinfo_type;
};

struct ThreadInfoDeleter {
    void operator()(threadinfo*) const {}
};

using TreeType = Masstree::basic_table<default_query_table_params>;

int main(int argc, char* argv[]) {
    int n = 250 * 10000;
    int thread_num = 4;
    int opt = 0;
    while ((opt = getopt(argc, argv, "n:t:")) != -1) {
        switch (opt) {
            case 'n':
                n = atoi(optarg);
                break;
            case 't':
                thread_num = atoi(optarg);
                break;
            default: /* '?' */
                fprintf(stderr, "Usage: %s [-n requests per thread] [-t threads num] \n",
                        argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    std::cout << "Request Per Thread: " << n << std::endl;
    std::cout << "Threads Num: " << thread_num << std::endl;

    auto main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    main_ti->pthread() = pthread_self();

    auto tree = new TreeType;
    tree->initialize(*main_ti);

    std::vector<std::thread> threads;
    auto insert_start = std::chrono::steady_clock::now();
    for (int i = 0; i < thread_num; ++i) {
        std::thread t([=] {
            thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> ti(
                    threadinfo::make(threadinfo::TI_PROCESS, -1));

            ti->pthread() = pthread_self();
            char buf[32] = {'\0'};
            for (int j = 0; j < n; ++j) {
                snprintf(buf, 32, "%09d-%010d", i, j);
                Masstree::Str key(buf);
                TreeType::cursor_type lp(*tree, key);
                lp.find_insert(*ti);
                lp.value() = new std::string(buf);
                lp.finish(1, *ti);
            }
        });
        threads.push_back(std::move(t));
    }

    for (auto& t: threads) {
        t.join();
    }

    auto insert_end = std::chrono::steady_clock::now();
    auto insert_used = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start).count();
    std::cout << "############ PUT " <<  n * thread_num << " requests used: " << insert_used << "ms, ops: "
              <<  ((uint64_t)n * thread_num * 1000)/ insert_used << std::endl;

    threads.clear();
    auto get_start = std::chrono::steady_clock::now();
    for (int i = 0; i < thread_num; ++i) {
        std::thread t([=] {
            thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> ti(
                    threadinfo::make(threadinfo::TI_PROCESS, -1));

            char buf[32] = {'\0'};
            for (int j = 0; j < n; ++j) {
                snprintf(buf, 32, "%09d-%010d", i, j);
                Masstree::Str key(buf);
                TreeType::value_type value;
                auto ret = tree->get(key, value, *ti);
                if (!ret) {
                    std::cerr << "Get " << buf << " return empty" << std::endl;
                    exit(EXIT_FAILURE);
                } else if (*value != buf) {
                    std::cerr << "Get " << buf << " wrong value: " << *value << std::endl;
                    exit(EXIT_FAILURE);
                } else {
//                    std::cout << "Get " << buf << " return " << *value << std::endl;
                }
            }
        });
        threads.push_back(std::move(t));
    }

    for (auto& t: threads) {
        t.join();
    }
    auto get_end = std::chrono::steady_clock::now();
    auto get_used = std::chrono::duration_cast<std::chrono::milliseconds>(get_end - get_start).count();
    std::cout << "############ Get " <<  n * thread_num << " requests used: " << get_used << "ms, ops: "
              <<  ((uint64_t)n * thread_num * 1000)/ get_used << std::endl;

    delete tree;

    return 0;
}
