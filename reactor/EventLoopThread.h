//
// EventLoopThread — 一个 EventLoop + 一个专用线程
//
// "one loop per thread" 的基本单元：
//   thread_.  ─→ EventLoop::loop()
//                 ↑
//   startLoop()  ┘ 返回 EventLoop* 给调用方
//
// 使用 condition_variable 保证 startLoop() 在 loop 启动完成后才返回。
//
#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <string>

class EventLoop;

class EventLoopThread {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    explicit EventLoopThread(ThreadInitCallback cb = ThreadInitCallback(),
                             std::string name = std::string());
    ~EventLoopThread();

    // 禁止拷贝
    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // 启动线程，运行 EventLoop::loop()
    // 返回该 EventLoop* 指针（供其他线程向其投递任务）
    EventLoop* startLoop();

private:
    void threadFunc();

    EventLoop* loop_;                     // loop_ 由 thread_ 管理
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
    ThreadInitCallback callback_;
    std::string name_;
    bool exiting_;
};
