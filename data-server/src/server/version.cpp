#include "version_gen.h"

#include <string>
#include <sstream>

namespace fbase {
namespace dataserver {
namespace server {

std::string GetGitDescribe() {
    return GIT_DESC;
}

std::string GetVersionInfo() {
    std::ostringstream ss;
    ss << "Git Describe:\t" << GIT_DESC << std::endl;
    ss << "Build Type:\t" << BUILD_DATE << std::endl;
    ss << "Build Date:\t" << BUILD_TYPE;
    return ss.str();
}

} /* namespace server */
} /* namespace dataserver */
} /* namespace fbase */
