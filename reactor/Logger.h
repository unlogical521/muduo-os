//
// Logger — 日志前端入口（仿 muduo）
//
// 使用方式：
//   LOG_INFO << "connection " << conn->name() << " established";
//   LOG_ERROR << "accept failed, errno=" << errno;
//
// 每个 LOG_XXX 展开为"临时 Logger 对象 + .stream()"：
//   LOG_INFO  →  Logger(__FILE__, __LINE__, Logger::INFO).stream() << ...
//
// 工作流程：
//   1. Logger 构造：往线程局部 LogStream 写格式头
//        "2026-08-11 12:00:00.123456 12345 INFO  basename.cpp:42 - "
//   2. .stream() << ... ：内容写入该 LogStream（thread_local，零锁）
//   3. Logger 析构：补 \n → 调用全局 g_output 输出
//        g_output 默认写 stderr（库可独立使用）；也可 setOutput 切到异步后端
//
// 级别过滤：
//   TRACE < DEBUG < INFO < WARN < ERROR < FATAL，全局默认 INFO。
//   被过滤的日志通过 LogStream::discard 丢弃写入，避免无效格式化。
//
#pragma once

#include <functional>
#include <string>
#include "LogStream.h"

class Logger {
public:
    enum LogLevel {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL,
    };

    // 输出 / 刷新函数指针（可被替换，如接到 AsyncLogging）
    using OutputFunc = std::function<void(const char*, int)>;
    using FlushFunc = std::function<void()>;

    // 构造：立即写格式头（时间戳/线程id/级别/位置）
    Logger(const char* file, int line, LogLevel level);
    ~Logger();

    // 禁止拷贝
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    // 返回日志流（被过滤时返回 discard 状态流，写入被丢弃）
    LogStream& stream() { return impl_.stream_; }

    // ========== 全局配置（线程安全：应在启动早期单线程调用） ==========
    static void setLogLevel(LogLevel level) { g_logLevel = level; }
    static LogLevel logLevel() { return g_logLevel; }

    // 设置输出/刷新目标。不调用时默认输出到 stderr（同步）。
    static void setOutput(OutputFunc fn) { g_output = std::move(fn); }
    static void setFlush(FlushFunc fn)   { g_flush  = std::move(fn); }

private:
    class Impl {
    public:
        Impl(const char* file, int line, LogLevel level);
        void finish();      // 写格式头
        LogStream stream_;
        LogLevel level() const { return level_; }

    private:
        const char* basename_;   // file 去掉路径后的文件名
        int line_;
        LogLevel level_;
    };

    static LogLevel g_logLevel;
    static OutputFunc g_output;
    static FlushFunc g_flush;

    Impl impl_;
};

// ========== 宏 ==========
// 展开为"临时 Logger + .stream()"，用法与 std::cout << 一致。
// 用 basename(__FILE__) 生成的文件名，减少日志体积。
#define LOG_TRACE Logger(__FILE__, __LINE__, Logger::TRACE).stream()
#define LOG_DEBUG Logger(__FILE__, __LINE__, Logger::DEBUG).stream()
#define LOG_INFO  Logger(__FILE__, __LINE__, Logger::INFO).stream()
#define LOG_WARN  Logger(__FILE__, __LINE__, Logger::WARN).stream()
#define LOG_ERROR Logger(__FILE__, __LINE__, Logger::ERROR).stream()
#define LOG_FATAL Logger(__FILE__, __LINE__, Logger::FATAL).stream()
