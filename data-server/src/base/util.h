#ifndef FBASE_BASE_UTIL_H_
#define FBASE_BASE_UTIL_H_

#include <sys/types.h>
#include <string>
#include <vector>

namespace fbase {

int randomInt();

std::string randomString(size_t length);

std::string strErrno(int errno_copy);

std::string SliceSeparate(const std::string &l, const std::string &r,
                          size_t max_len = 0);

std::string JoinFilePath(const std::vector<std::string> &strs);

int MakeDirAll(const std::string &path, mode_t mode);

int RemoveDirAll(const char *name);

void AnnotateThread(pthread_t handle, const char *name);

} /* namespace fbase */

#endif /* end of include guard: FBASE_BASE_UTIL_H_ */
