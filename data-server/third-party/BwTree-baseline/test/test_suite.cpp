
/*
 * test_suite.cpp
 *
 * This files includes basic testing infrastructure
 *
 * by Ziqi Wang
 */

#include "test_suite.h"

/*
 * GetEmptyTree() - Return an empty BwTree with proper constructor argument
 *                  in order to finish all tests without problem
 *
 * This function will switch print_flag on and off before and after calling
 * the constructor, in order to print tree metadata under debug mode
 */
TreeType *GetEmptyTree(bool no_print) {
  if(no_print == false) {
    print_flag = true;
  }
  
  TreeType *t1 = new TreeType{true,
                              KeyComparator{1},
                              KeyEqualityChecker{1}};
                      
  print_flag = false;
  
  return t1;
}

/*
 * DestroyTree() - Deletes a tree and release all resources
 *
 * This function will enable and disable print flag before and after
 * calling the destructor in order to print out the process of
 * tree destruction under debug mode
 */
void DestroyTree(TreeType *t, bool no_print) {
  if(no_print == false) {
    print_flag = true;
  }
  
  delete t;
  
  print_flag = false;
  
  return;
}

/*
 * PrintStat() - Print the current statical information on stdout
 */
void PrintStat(TreeType *t) {
  printf("Insert op = %lu; abort = %lu; abort rate = %lf\n",
         t->insert_op_count.load(),
         t->insert_abort_count.load(),
         (double)t->insert_abort_count.load() / (double)t->insert_op_count.load());

  printf("Delete op = %lu; abort = %lu; abort rate = %lf\n",
         t->delete_op_count.load(),
         t->delete_abort_count.load(),
         (double)t->delete_abort_count.load() / (double)t->delete_op_count.load());

  return;
}

/*
 * PinToCore() - Pin the current calling thread to a particular core
 */
void PinToCore(size_t core_id) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(core_id, &cpu_set);

  int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);

  printf("pthread_setaffinity_np() returns %d\n", ret);

  return;
}
