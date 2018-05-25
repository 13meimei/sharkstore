_Pragma("once");

namespace fbase {
namespace raft {

class Logger {
public:
    Logger() {}
    virtual ~Logger() {}

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

public:
    virtual bool IsEnableDebug() = 0;
    virtual bool IsEnableInfo() = 0;
    virtual bool IsEnableWarn() = 0;

    virtual void Debug(const char* file, int line, const char* format, ...) = 0;
    virtual void Info(const char* file, int line, const char* format, ...) = 0;
    virtual void Warn(const char* file, int line, const char* format, ...) = 0;
    virtual void Error(const char* file, int line, const char* format, ...) = 0;
};

void SetLogger(Logger* logger);

} /* namespace raft */
} /* namespace fbase */
