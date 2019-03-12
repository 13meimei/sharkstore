
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure and function declarations
 *
 * by Ziqi Wang
 */

#include <cstring>
#include <string>
#include <unordered_map>
#include <random>
#include <map>
#include <fstream>
#include <iostream>

#include <pthread.h>

#include "../src/bwtree.h"
#include "../benchmark/stx_btree/btree.h"
#include "../benchmark/stx_btree/btree_multimap.h"
#include "../benchmark/libcuckoo/cuckoohash_map.hh"

#ifdef BWTREE_PELOTON
using namespace peloton::index;
#endif

using namespace stx;

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

using TreeType = BwTree<long int,
                        long int,
                        KeyComparator,
                        KeyEqualityChecker>;
using LeafRemoveNode = typename TreeType::LeafRemoveNode;
using LeafInsertNode = typename TreeType::LeafInsertNode;
using LeafDeleteNode = typename TreeType::LeafDeleteNode;
using LeafSplitNode = typename TreeType::LeafSplitNode;
using LeafMergeNode = typename TreeType::LeafMergeNode;
using LeafNode = typename TreeType::LeafNode;

using InnerRemoveNode = typename TreeType::InnerRemoveNode;
using InnerInsertNode = typename TreeType::InnerInsertNode;
using InnerDeleteNode = typename TreeType::InnerDeleteNode;
using InnerSplitNode = typename TreeType::InnerSplitNode;
using InnerMergeNode = typename TreeType::InnerMergeNode;
using InnerNode = typename TreeType::InnerNode;

using DeltaNode = typename TreeType::DeltaNode;

using NodeType = typename TreeType::NodeType;
using ValueSet = typename TreeType::ValueSet;
using NodeSnapshot = typename TreeType::NodeSnapshot;
using BaseNode = typename TreeType::BaseNode;

using Context = typename TreeType::Context;

/*
 * Common Infrastructure
 */
 
#define END_TEST do{ \
                print_flag = true; \
                delete t1; \
                \
                return 0; \
               }while(0);
 
/*
 * LaunchParallelTestID() - Starts threads on a common procedure
 *
 * This function is coded to be accepting variable arguments
 *
 * NOTE: Template function could only be defined in the header
 */
template <typename Fn, typename... Args>
void LaunchParallelTestID(uint64_t num_threads, Fn&& fn, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(fn, thread_itr, args...));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

/*
 * class SimpleInt64Random - Simple paeudo-random number generator 
 *
 * This generator does not have any performance bottlenect even under
 * multithreaded environment, since it only uses local states. It hashes
 * a given integer into a value between 0 - UINT64T_MAX, and in order to derive
 * a number inside range [lower bound, upper bound) we should do a mod and 
 * addition
 *
 * This function's hash method takes a seed for generating a hashing value,
 * together with a salt which is used to distinguish different callers
 * (e.g. threads). Each thread has a thread ID passed in the inlined hash
 * method (so it does not pose any overhead since it is very likely to be 
 * optimized as a register resident variable). After hashing finishes we just
 * normalize the result which is evenly distributed between 0 and UINT64_T MAX
 * to make it inside the actual range inside template argument (since the range
 * is specified as template arguments, they could be unfold as constants during
 * compilation)
 *
 * Please note that here upper is not inclusive (i.e. it will not appear as the 
 * random number)
 */
template <uint64_t lower, uint64_t upper>
class SimpleInt64Random {
 public:
   
  /*
   * operator()() - Mimics function call
   *
   * Note that this function must be denoted as const since in STL all
   * hashers are stored as a constant object
   */
  inline uint64_t operator()(uint64_t value, uint64_t salt) const {
    //
    // The following code segment is copied from MurmurHash3, and is used
    // as an answer on the Internet:
    // http://stackoverflow.com/questions/5085915/what-is-the-best-hash-
    //   function-for-uint64-t-keys-ranging-from-0-to-its-max-value
    //
    // For small values this does not actually have any effect
    // since after ">> 33" all its bits are zeros
    //value ^= value >> 33;
    value += salt;
    value *= 0xff51afd7ed558ccd;
    value ^= value >> 33;
    value += salt;
    value *= 0xc4ceb9fe1a85ec53;
    value ^= value >> 33;

    return lower + value % (upper - lower);
  }
};

/*
 * class Timer - Measures time usage for testing purpose
 */
class Timer {
 private:
  std::chrono::time_point<std::chrono::system_clock> start;
  std::chrono::time_point<std::chrono::system_clock> end;
  
 public: 
 
  /* 
   * Constructor
   *
   * It takes an argument, which denotes whether the timer should start 
   * immediately. By default it is true
   */
  Timer(bool start = true) : 
    start{},
    end{} {
    if(start == true) {
      Start();
    }
    
    return;
  }
  
  /*
   * Start() - Starts timer until Stop() is called
   *
   * Calling this multiple times without stopping it first will reset and
   * restart
   */
  inline void Start() {
    start = std::chrono::system_clock::now();
    
    return;
  }
  
  /*
   * Stop() - Stops timer and returns the duration between the previous Start()
   *          and the current Stop()
   *
   * Return value is represented in double, and is seconds elapsed between
   * the last Start() and this Stop()
   */
  inline double Stop() {
    end = std::chrono::system_clock::now();
    
    return GetInterval();
  }
  
  /*
   * GetInterval() - Returns the length of the time interval between the latest
   *                 Start() and Stop()
   */
  inline double GetInterval() const {
    std::chrono::duration<double> elapsed_seconds = end - start;
    return elapsed_seconds.count();
  }
};

/*
 * class Envp() - Reads environmental variables 
 */
class Envp {
 public:
  /*
   * Get() - Returns a string representing the value of the given key
   *
   * If the key does not exist then just use empty string. Since the value of 
   * an environmental key could not be empty string
   */
  static std::string Get(const std::string &key) {
    char *ret = getenv(key.c_str());
    if(ret == nullptr) {
      return std::string{""}; 
    } 
    
    return std::string{ret};
  }
  
  /*
   * operator() - This is called with an instance rather than class name
   */
  std::string operator()(const std::string &key) const {
    return Envp::Get(key);
  }
  
  /*
   * GetValueAsUL() - Returns the value by argument as unsigned long
   *
   * If the env var is found and the value is parsed correctly then return true 
   * If the env var is not found then retrun true, and value_p is not modified
   * If the env var is found but value could not be parsed correctly then
   *   return false and value is not modified 
   */
  static bool GetValueAsUL(const std::string &key, 
                           unsigned long *value_p) {
    const std::string value = Envp::Get(key);
    
    // Probe first character - if is '\0' then we know length == 0
    if(value.c_str()[0] == '\0') {
      return true;
    }
    
    unsigned long result;
    
    try {
      result = std::stoul(value);
    } catch(...) {
      return false; 
    } 
    
    *value_p = result;
    
    return true;
  }
};

/*
 * class Zipfian - Generates zipfian random numbers
 *
 * This class is adapted from: 
 *   https://github.com/efficient/msls-eval/blob/master/zipf.h
 *   https://github.com/efficient/msls-eval/blob/master/util.h
 *
 * The license is Apache 2.0.
 *
 * Usage:
 *   theta = 0 gives a uniform distribution.
 *   0 < theta < 0.992 gives some Zipf dist (higher theta = more skew).
 * 
 * YCSB's default is 0.99.
 * It does not support theta > 0.992 because fast approximation used in
 * the code cannot handle that range.
  
 * As extensions,
 *   theta = -1 gives a monotonely increasing sequence with wraparounds at n.
 *   theta >= 40 returns a single key (key 0) only. 
 */
class Zipfian {
 private:
  // number of items (input)
  uint64_t n;    
  // skewness (input) in (0, 1); or, 0 = uniform, 1 = always zero
  double theta;  
  // only depends on theta
  double alpha;  
  // only depends on theta
  double thres;
  // last n used to calculate the following
  uint64_t last_n;  
  
  double dbl_n;
  double zetan;
  double eta;
  uint64_t rand_state; 
 
  /*
   * PowApprox() - Approximate power function
   *
   * This function is adapted from the above link, which was again adapted from
   *   http://martin.ankerl.com/2012/01/25/optimized-approximative-pow-in-c-and-cpp/
   */
  static double PowApprox(double a, double b) {
    // calculate approximation with fraction of the exponent
    int e = (int)b;
    union {
      double d;
      int x[2];
    } u = {a};
    u.x[1] = (int)((b - (double)e) * (double)(u.x[1] - 1072632447) + 1072632447.);
    u.x[0] = 0;
  
    // exponentiation by squaring with the exponent's integer part
    // double r = u.d makes everything much slower, not sure why
    // TODO: use popcount?
    double r = 1.;
    while (e) {
      if (e & 1) r *= a;
      a *= a;
      e >>= 1;
    }
  
    return r * u.d;
  }
  
  /*
   * Zeta() - Computes zeta function
   */
  static double Zeta(uint64_t last_n, double last_sum, uint64_t n, double theta) {
    if (last_n > n) {
      last_n = 0;
      last_sum = 0.;
    }
    
    while (last_n < n) {
      last_sum += 1. / PowApprox((double)last_n + 1., theta);
      last_n++;
    }
    
    return last_sum;
  }
  
  /*
   * FastRandD() - Fast randum number generator that returns double
   *
   * This is adapted from:
   *   https://github.com/efficient/msls-eval/blob/master/util.h
   */
  static double FastRandD(uint64_t *state) {
    *state = (*state * 0x5deece66dUL + 0xbUL) & ((1UL << 48) - 1);
    return (double)*state / (double)((1UL << 48) - 1);
  }
 
 public:

  /*
   * Constructor
   *
   * Note that since we copy this from C code, either memset() or the variable
   * n having the same name as a member is a problem brought about by the
   * transformation
   */
  Zipfian(uint64_t n, double theta, uint64_t rand_seed) {
    assert(n > 0);
    if (theta > 0.992 && theta < 1) {
      fprintf(stderr, "theta > 0.992 will be inaccurate due to approximation\n");
    } else if (theta >= 1. && theta < 40.) {
      fprintf(stderr, "theta in [1., 40.) is not supported\n");
      assert(false);
    }
    
    assert(theta == -1. || (theta >= 0. && theta < 1.) || theta >= 40.);
    assert(rand_seed < (1UL << 48));
    
    // This is ugly, but it is copied from C code, so let's preserve this
    memset(this, 0, sizeof(*this));
    
    this->n = n;
    this->theta = theta;
    
    if (theta == -1.) { 
      rand_seed = rand_seed % n;
    } else if (theta > 0. && theta < 1.) {
      this->alpha = 1. / (1. - theta);
      this->thres = 1. + PowApprox(0.5, theta);
    } else {
      this->alpha = 0.;  // unused
      this->thres = 0.;  // unused
    }
    
    this->last_n = 0;
    this->zetan = 0.;
    this->rand_state = rand_seed;
    
    return;
  }
  
  /*
   * ChangeN() - Changes the parameter n after initialization
   *
   * This is adapted from zipf_change_n()
   */
  void ChangeN(uint64_t n) {
    this->n = n;
    
    return;
  }
  
  /*
   * Get() - Return the next number in the Zipfian distribution
   */
  uint64_t Get() {
    if (this->last_n != this->n) {
      if (this->theta > 0. && this->theta < 1.) {
        this->zetan = Zeta(this->last_n, this->zetan, this->n, this->theta);
        this->eta = (1. - PowApprox(2. / (double)this->n, 1. - this->theta)) /
                     (1. - Zeta(0, 0., 2, this->theta) / this->zetan);
      }
      this->last_n = this->n;
      this->dbl_n = (double)this->n;
    }
  
    if (this->theta == -1.) {
      uint64_t v = this->rand_state;
      if (++this->rand_state >= this->n) this->rand_state = 0;
      return v;
    } else if (this->theta == 0.) {
      double u = FastRandD(&this->rand_state);
      return (uint64_t)(this->dbl_n * u);
    } else if (this->theta >= 40.) {
      return 0UL;
    } else {
      // from J. Gray et al. Quickly generating billion-record synthetic
      // databases. In SIGMOD, 1994.
  
      // double u = erand48(this->rand_state);
      double u = FastRandD(&this->rand_state);
      double uz = u * this->zetan;
      
      if(uz < 1.) {
        return 0UL;
      } else if(uz < this->thres) {
        return 1UL;
      } else {
        return (uint64_t)(this->dbl_n *
                          PowApprox(this->eta * (u - 1.) + 1., this->alpha));
      }
    }
    
    // Should not reach here
    assert(false);
    return 0UL;
  }
   
};

TreeType *GetEmptyTree(bool no_print = false);
void DestroyTree(TreeType *t, bool no_print = false);
void PrintStat(TreeType *t);
void PinToCore(size_t core_id);

/*
 * Basic test suite
 */
void InsertTest1(uint64_t thread_id, TreeType *t);
void InsertTest2(uint64_t thread_id, TreeType *t);
void DeleteTest1(uint64_t thread_id, TreeType *t);
void DeleteTest2(uint64_t thread_id, TreeType *t);

void InsertGetValueTest(TreeType *t);
void DeleteGetValueTest(TreeType *t);

extern int basic_test_key_num;
extern int basic_test_thread_num;

/*
 * Mixed test suite
 */
void MixedTest1(uint64_t thread_id, TreeType *t);
void MixedGetValueTest(TreeType *t);

extern std::atomic<size_t> mixed_insert_success;
extern std::atomic<size_t> mixed_delete_success;
extern std::atomic<size_t> mixed_delete_attempt;

extern int mixed_thread_num;
extern int mixed_key_num;

/*
 * Performance test suite
 */
void TestStdMapInsertReadPerformance(int key_size);
void TestStdUnorderedMapInsertReadPerformance(int key_size);
void TestBTreeInsertReadPerformance(int key_size);
void TestBTreeMultimapInsertReadPerformance(int key_size);
void TestCuckooHashTableInsertReadPerformance(int key_size);
void TestBwTreeInsertReadDeletePerformance(TreeType *t, int key_num);
void TestBwTreeInsertReadPerformance(TreeType *t, int key_num);

// Multithreaded benchmark
void BenchmarkBwTreeSeqInsert(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeSeqRead(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeRandRead(TreeType *t, int key_num, int thread_num);
void BenchmarkBwTreeZipfRead(TreeType *t, int key_num, int thread_num);

void TestBwTreeEmailInsertPerformance(BwTree<std::string, long int> *t, std::string filename);

/*
 * Stress test suite
 */
void StressTest(uint64_t thread_id, TreeType *t);

/*
 * Iterator test suite
 */
void IteratorTest(TreeType *t);

/*
 * Random test suite
 */
void RandomBtreeMultimapInsertSpeedTest(size_t key_num);
void RandomCuckooHashMapInsertSpeedTest(size_t key_num);
void RandomInsertSpeedTest(TreeType *t, size_t key_num);
void RandomInsertSeqReadSpeedTest(TreeType *t, size_t key_num);
void SeqInsertRandomReadSpeedTest(TreeType *t, size_t key_num);
void InfiniteRandomInsertTest(TreeType *t);
void RandomInsertTest(uint64_t thread_id, TreeType *t);
void RandomInsertVerify(TreeType *t);

/*
 * Misc test suite
 */
void TestEpochManager(TreeType *t);

