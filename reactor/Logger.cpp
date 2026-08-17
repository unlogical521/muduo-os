//
// Logger 实现
//
#include "Logger.h"

#include <sys/syscall.h>
#include <unistd.h>
#include <time.h>
#include <cstdio>
#include <cstdlib>
#include <cerrno>

// ========== 全局静态配置 ==========
// g_logLevel 默认 INFO：TRACE/DEBUG 被过滤
Logger::LogLevel Logger::g_logLevel = Logger::INFO;
// 默认输出到 stderr（同步）——未 setOutput 时库可独立使用
Logger::OutputFunc Logger::g_output =
    [](const char* msg, int len) { fwrite(msg, 1, len, stderr); };
Logger::FlushFunc Logger::g_flush =
    []() { fflush(stderr); };

namespace {

// 取 __FILE__ 去掉路径后的文件名（返回指针指向路径末尾，无堆分配）
// 注：不能叫 basename()，与 <string.h> 的 POSIX 函数 basename() 冲突
const char* stripPath(const char* file) {
    const char* slash = strrchr(file, '/');
    return slash ? slash + 1 : file;
}

// 当前线程的内核线程 id（对应 /proc/<pid>/task/<tid>）
// thread_local 缓存格式化结果，避免每条日志重复系统调用
const std::string& tidString() {
    thread_local std::string t_tid;
    thread_local pid_t t_tidValue = 0;
    pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
    if (tid != t_tidValue) {
        t_tidValue = tid;
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%d", tid);
        t_tid.assign(buf, len);
    }
    return t_tid;
}

// 级别 → 字符串（定宽，日志对齐用）
const char* levelString(Logger::LogLevel level) {
    static const char* kLevels[] = {
        "TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL",
    };
    return kLevels[level];
}

// 时间戳：YYYY-MM-DD HH:MM:SS.microseconds（真实时间，本地时区）
void formatTime(LogStream& stream) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    struct tm tmv;
    time_t seconds = ts.tv_sec;
    localtime_r(&seconds, &tmv);   // 线程安全版本

    char buf[64];
    int len = snprintf(buf, sizeof(buf),
                       "%04d-%02d-%02d %02d:%02d:%02d.%06ld ",
                       tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                       tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
                       ts.tv_nsec / 1000);
    stream << buf;
}

}  // namespace

// ========== Impl ==========
Logger::Impl::Impl(const char* file, int line, LogLevel level)
    : stream_(), basename_(stripPath(file)), line_(line), level_(level) {
    // 第一步就捕获 errno，避免后续 clock_gettime/localtime_r 等调用覆盖它
    int savedErrno = errno;

    formatTime(stream_);
    stream_ << tidString() << ' ';
    stream_ << levelString(level) << ' ';
    stream_ << basename_ << ':' << line_ << " - ";

    // 错误级日志附带 errno（若调用点处于错误分支）
    if (savedErrno != 0 && level >= Logger::ERROR) {
        stream_ << "(errno=" << savedErrno << ") ";
    }

    // 级别低于全局阈值 → 本流丢弃后续写入
    if (g_logLevel > level) {
        stream_.discard(true);
    }
}

// ========== Logger ==========
Logger::Logger(const char* file, int line, LogLevel level)
    : impl_(file, line, level) {}

Logger::~Logger() {
    if (!impl_.stream_.discard()) {
        // 补换行 → 通过 g_output 输出（默认 stderr，或异步后端）
        impl_.stream_ << '\n';
        const LogStream::Buffer& buf = impl_.stream_.buffer();
        g_output(buf.data(), buf.length());

        // 错误级日志立即刷新，保证及时可见
        if (impl_.level() >= ERROR) {
            g_flush();
        }
    }

    // FATAL：致命错误，输出后终止进程
    if (impl_.level() == FATAL) {
        abort();
    }
}
