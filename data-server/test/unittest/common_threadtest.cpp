#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <cstdlib>

#include "base/util.h"
#include "server/common_thread.h"

using namespace sharkstore::dataserver::server; 

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

TEST( COMMON_THREAD, common_thrad_func_submit) {
    std::srand(std::time(0));
    const size_t queue_cap = std::rand()%100+1; 
    std::atomic<int> ct = {0};

    /* test 1 */
    CommonThread thr(queue_cap, "common_thread_1 test");
    std::string str_cmd = "command";
    std::atomic<bool> running = {false};
    std::function<void(void)> f0 = [&ct](void) { 
//        std::cerr << "test 1 output:{" << ct << "}" << std::endl;
        ct.fetch_add(1); 
    };
    
    for (size_t i = 0 ; i< queue_cap ; i++) {
        ASSERT_TRUE(thr.submit( i, &running, f0, str_cmd)); 
    }
    
    size_t count = 0;
    while(thr.size() > 0 && count < queue_cap ) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 
    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap , ct.load());

    ct.fetch_sub(queue_cap);
    ASSERT_EQ(ct.load(), 0);

    /* test 2  */
    std::mutex mu;
    std::condition_variable cv;

    std::function<void(void)> f1 = [&mu, &cv, &ct](void) {
        
//        std::cerr << "test 2 td:{" << ct << "}" << std::endl;
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock);
//        std::cerr << "test 2 output:{" << ct << "}" << std::endl;
        
        ct.fetch_add(1);
    }; 
    
    mu.lock();
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        ASSERT_TRUE(thr.submit(i, &running, f1, str_cmd));
    } 

    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    ASSERT_EQ(thr.size(), queue_cap);
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        ASSERT_FALSE(thr.submit(i, &running, f1, str_cmd));
    }
    ASSERT_EQ(thr.size(), queue_cap );

    mu.unlock();
    for (size_t i = 0; i < queue_cap + 2; i++ ) {
        cv.notify_one();
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
    
    count = 0;
    while(thr.size() > 0 && count < queue_cap+1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 

    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap + 1, ct.load());
    
    ct.fetch_sub(queue_cap + 1);
    ASSERT_EQ(ct.load(), 0);

    /* test 3 */
    thr.shutdown();

    for (size_t i = 0; i < queue_cap - 1; i++ ) {
        ASSERT_FALSE(thr.submit(i, &running, f0, str_cmd));
    }
    ASSERT_EQ(thr.size(), 0); 

}

TEST( COMMON_THREAD, common_thrad_func_tryPost) {

    std::srand(std::time(0));
    const size_t queue_cap = std::rand()%100+1; 
    std::atomic<int> ct = {0};

    /* test 1 */
    CommonThread thr(queue_cap, "common_thread_1 test");
    std::string str_cmd = "command";
    std::atomic<bool> running = {false};
    std::function<void(void)> f0 = [&ct](void) { 
//        std::cerr << "test 1 output:{" << ct << "}" << std::endl;
        ct.fetch_add(1); 
    };
    
    for (size_t i = 0 ; i< queue_cap ; i++) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        ASSERT_TRUE(thr.tryPost(cw)); 
    }
    
    size_t count = 0;
    while(thr.size() > 0 && count < queue_cap ) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 
    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap , ct.load());

    ct.fetch_sub(queue_cap);
    ASSERT_EQ(ct.load(), 0);

    /* test 2  */
    std::mutex mu;
    std::condition_variable cv;

    std::function<void(void)> f1 = [&mu, &cv, &ct](void) {
        
//       std::cerr << "test 2 td:{" << ct << "}" << std::endl;
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock);
//        std::cerr << "test 2 output:{" << ct << "}" << std::endl;
        
        ct.fetch_add(1);
    }; 
    
    mu.lock();
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f1; 
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        ASSERT_TRUE(thr.tryPost(cw)); 
    } 

    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    ASSERT_EQ(thr.size(), queue_cap);
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f1; 
        
        ASSERT_FALSE(thr.tryPost(cw)); 
    }
    ASSERT_EQ(thr.size(), queue_cap );

    mu.unlock();
    for (size_t i = 0; i < queue_cap + 2; i++ ) {
        cv.notify_one();
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
    
    count = 0;
    while(thr.size() > 0 && count < queue_cap+1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 

    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap + 1, ct.load());
    
    ct.fetch_sub(queue_cap + 1);
    ASSERT_EQ(ct.load(), 0);

    /* test 3 */
    thr.shutdown();

    for (size_t i = 0; i < queue_cap - 1; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        ASSERT_FALSE(thr.tryPost(cw)); 
    }
    ASSERT_EQ(thr.size(), 0); 
}


TEST( COMMON_THREAD, common_thrad_func_post) {

    std::srand(std::time(0));
    const size_t queue_cap = std::rand()%100+1; 
    std::atomic<int> ct = {0};

    /* test 1 */
    CommonThread thr(queue_cap, "common_thread_1 test");
    std::string str_cmd = "command";
    std::atomic<bool> running = {false};
    std::function<void(void)> f0 = [&ct](void) { 
//        std::cerr << "test 1 output:{" << ct << "}" << std::endl;
        ct.fetch_add(1); 
    };
    
    for (size_t i = 0 ; i< queue_cap ; i++) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        thr.post(cw); 
    }
    
    size_t count = 0;
    while(thr.size() > 0 && count < queue_cap ) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 
    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap , ct.load());

    ct.fetch_sub(queue_cap);
    ASSERT_EQ(ct.load(), 0);

    /* test 2  */
    std::mutex mu;
    std::condition_variable cv;

    std::function<void(void)> f1 = [&mu, &cv, &ct](void) {
        
//        std::cerr << "test 2 td:{" << ct << "}" << std::endl;
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock);
//        std::cerr << "test 2 output:{" << ct << "}" << std::endl;
        
        ct.fetch_add(1);
    }; 
    
    mu.lock();
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f1; 
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        thr.post(cw); 
    } 

    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    ASSERT_EQ(thr.size(), queue_cap);
    for (size_t i = 0; i < queue_cap+1 ; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f1; 
        
        thr.post(cw); 
    }
    ASSERT_EQ(thr.size(), 2*queue_cap + 2 - 1);

    mu.unlock();
    for (size_t i = 0; i < 2*queue_cap + 3; i++ ) {
        cv.notify_one();
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
    
    count = 0;
    while(thr.size() > 0 && count < queue_cap+1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 

    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(2*queue_cap + 2, ct.load());
    
    ct.fetch_sub(2*queue_cap + 2);
    ASSERT_EQ(ct.load(), 0);

    /* test 3 */
    thr.shutdown();

    for (size_t i = 0; i < queue_cap - 1; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        thr.post(cw); 
    }
    ASSERT_EQ(thr.size(), 0); 
}

TEST( COMMON_THREAD, common_thrad_func_waitPost) {

    std::srand(std::time(0));
    const size_t queue_cap = std::rand()%100+1; 
    std::atomic<int> ct = {0};

    /* test 1 */
    CommonThread thr(queue_cap, "common_thread_1 test");
    std::string str_cmd = "command";
    std::atomic<bool> running = {false};
    std::function<void(void)> f0 = [&ct](void) { 
//        std::cerr << "test 1 output:{" << ct << "}" << std::endl;
        ct.fetch_add(1); 
    };
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    for (size_t i = 0 ; i< queue_cap ; i++) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        thr.waitPost(cw); 
    }
    
    size_t count = 0;
    while(thr.size() > 0 && count < queue_cap ) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 
    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(queue_cap , ct.load());

    ct.fetch_sub(queue_cap);
    ASSERT_EQ(ct.load(), 0);

    /* test 2  */

    for (size_t i = 0; i < 2*queue_cap+1 ; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        thr.waitPost(cw); 
    } 

    count = 0;
    while(thr.size() > 0 && count < 2*queue_cap+1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
        count++; 
    } 

    ASSERT_EQ(thr.size(), 0);
    ASSERT_EQ(2*queue_cap + 1, ct.load());
    
    ct.fetch_sub(2*queue_cap + 1);
    ASSERT_EQ(ct.load(), 0);

    /* test 3 */
    thr.shutdown();

    for (size_t i = 0; i < queue_cap - 1; i++ ) {
        CommonWork cw;
        cw.owner = i;
        cw.running = &running;
        cw.f0 = f0; 
        
        thr.waitPost(cw); 
    }
    ASSERT_EQ(thr.size(), 0); 
}



} /* namespace  */
