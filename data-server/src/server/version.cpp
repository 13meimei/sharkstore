#include <string>
#include <sstream>

#include "version_gen.h"

namespace sharkstore {
namespace dataserver {
namespace server {

std::string GetGitDescribe() {
    return GIT_DESC;
}

std::string GetVersionInfo() {
    std::ostringstream ss;
    ss << "Git Describe:\t" << GIT_DESC << std::endl;
    ss << "Build Date:\t" << BUILD_DATE << std::endl;
    ss << "Build Type:\t" << BUILD_TYPE << std::endl;
    ss << "Build Flags:\t" << BUILD_FLAGS;
    return ss.str();
}

} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */
