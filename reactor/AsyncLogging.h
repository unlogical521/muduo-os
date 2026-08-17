//
// AsyncLogging — 异步日志后端（仿 muduo 双缓冲）
//
// 职责：接收前端 Logger 的输出，由独立后台线程落盘，磁盘 IO 不阻塞任何网络线程。
//
// 双缓冲机制：
//   前端（生产）：currentBuffer_ 当前写 / nextBuffer_ 备用
//   后端（消费）：newBuffer1 / newBuffer2 两个归还用缓冲
//
//   append()（任意线程，加锁）：
//     - currentBuffer_ 未满 → 直接拷贝，无锁等待
//     - 写满 → 把 currentBuffer_ 移入 buffers_ 队列，换 nextBuffer_ 继续，
//       并 notify 唤醒后端
//
//   threadFunc()（后台线程）：
//     - wait_for(flushInterval)：被唤醒或超时（定期 flush 兜底）
//     - 取走 buffers_ 整批 → 无锁写文件 → 归还缓冲区给前端 → flush
//
//   前端锁只持有在"缓冲交接"瞬间，后端锁只在取队列时持有；
//   磁盘写全程在后端线程，IO 线程永不碰磁盘。
//
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <memory>

#include "LogStream.h"   // 复用 FixedBuffer

class AsyncLogging {
public:
    // 后端缓冲用大缓冲（4MB）：前端积压一小段时间的数据都在这里攒着
    static const int kLargeBuffer = 4 * 1024 * 1024;

    using Buffer = muduo_detail::FixedBuffer<kLargeBuffer>;
    using BufferPtr = std::unique_ptr<Buffer>;
    using BufferVector = std::vector<BufferPtr>;

    // basename：日志文件前缀；rollSize：滚动阈值（字节）；flushInterval：定期落盘间隔（秒）
    AsyncLogging(const std::string& basename,
                 int rollSize = 100 * 1024 * 1024,
                 int flushInterval = 3);
    ~AsyncLogging();

    // 禁止拷贝
    AsyncLogging(const AsyncLogging&) = delete;
    AsyncLogging& operator=(const AsyncLogging&) = delete;

    // 前端调用（任意线程）——追加一条日志
    void append(const char* msg, int len);

    // 启动后端线程 / 停止（停止会等待落盘，保证退出前日志完整）
    void start();
    void stop();

    // 请求尽快落盘（唤醒后端立即取走当前缓冲写盘）
    void flush();

private:
    void threadFunc();
    void openFile();                                   // 打开（或滚动重开）日志文件
    void writeToFile(const char* data, int len);       // 写盘 + 按 rollSize 滚动
    void flushFile() { if (file_) fflush(file_); }

    const std::string basename_;
    const int rollSize_;
    const int flushInterval_;

    std::atomic<bool> running_;
    std::thread thread_;

    std::mutex mutex_;
    std::condition_variable cond_;

    // 前端持有的两组缓冲（受 mutex_ 保护）
    BufferPtr currentBuffer_;
    BufferPtr nextBuffer_;
    BufferVector buffers_;       // 已写满、待后端写盘的缓冲

    FILE* file_;                 // 日志文件句柄（仅后端线程访问）
    long long writtenBytes_;     // 当前文件已写字节（滚动判断用）
};
