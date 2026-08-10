//
// EventLoopThread — 一个 EventLoop + 一个专用线程
//
// "one loop per thread" 的基本单元：
//   thread_.  ─→ EventLoop::loop()
//                 ↑
//   startLoop()  ┘ 返回 EventLoop* 给调用方
//
// 使用 condition_variable 保证 startLoop() 在 loop 启动完成后才返回。
// 这是多 Reactor 线程池的基本构建块。
//
// 典型用法：
//   EventLoopThread t;                    // 1. 创建对象，尚未启动线程
//   EventLoop* loop = t.startLoop();      // 2. 启动线程，等待 loop 就绪
//   loop->runInLoop([...]{ ... });        // 3. 向该线程投递任务
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

    // 禁止拷贝（线程不可复制）
    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // 启动线程，运行 EventLoop::loop()
    // 返回该 EventLoop* 指针（供其他线程向其投递任务）
    // 线程安全：阻塞等待直到新线程上的 EventLoop 已就绪
    EventLoop* startLoop();

private:
    // 线程入口函数（在 thread_ 上运行）
    // 1. 创建栈上 EventLoop
    // 2. 调用 ThreadInitCallback（如果有）
    // 3. 通过 cond_ 通知 startLoop 线程 loop 已就绪
    // 4. 进入事件循环 loop.loop()
    void threadFunc();

    EventLoop* loop_;                     // 指向栈上 EventLoop（由 threadFunc 的栈管理）
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
    ThreadInitCallback callback_;
    std::string name_;
    bool exiting_;
};
