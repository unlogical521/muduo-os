//
// EventLoop — 事件循环（Reactor 的核心）
//
// 封装 epoll：
//   1. epoll_create
//   2. epoll_ctl   — 通过 Channel 的 update / remove
//   3. epoll_wait  — 在主循环中调用
//
// 多线程扩展：
//   - threadId_      → 记录所属线程
//   - wakeupFd_      → eventfd，跨线程唤醒
//   - pendingFunctors_ → 跨线程回调队列
//   - runInLoop()    → 如果在本线程直接执行，否则 queueInLoop
//   - queueInLoop()  → 入队 + 唤醒目标 EventLoop
//
// 运行逻辑：
//   while (!quit_) {
//       active_channels_ = epoll_wait(...)
//       for each channel in active_channels_:
//           channel->handleEvent()
//       doPendingFunctors()
//   }
//
#pragma once

#include <sys/epoll.h>
#include <functional>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>

class Channel;

class EventLoop {
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    // 禁止拷贝
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // 启动事件循环（阻塞，直到 quit() 被调用）
    void loop();

    // 退出事件循环（线程安全）
    void quit() { quit_ = true; wakeup(); }

    // 更新/移除 Channel 在 epoll 中的注册状态
    void updateChannel(Channel* ch);
    void removeChannel(Channel* ch);

    // 获取 epoll fd（给 Channel 等内部使用）
    int epfd() const { return epfd_; }

    // === 跨线程调度 ===

    // 如果当前线程正是 loop 线程，直接执行 cb；否则入队并唤醒
    void runInLoop(Functor cb);

    // 将 cb 入队到 pending 队列，唤醒 loop 所属线程
    void queueInLoop(Functor cb);

    // 判断调用者是否在本 EventLoop 的线程上
    bool isInLoopThread() const { return threadId_ == std::this_thread::get_id(); }

private:
    static const int kInitEventListSize = 64;

    void wakeup();            // 写 eventfd 唤醒 loop 线程
    void handleWakeup();      // 读 eventfd 消费唤醒事件
    void doPendingFunctors(); // 执行跨线程回调

    std::atomic<bool> quit_;
    int epfd_;
    std::vector<epoll_event> events_;
    std::map<int, Channel*> channels_;

    // 跨线程支持
    std::thread::id threadId_;
    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;
    std::vector<Functor> pendingFunctors_;
    std::mutex mutex_;
    bool callingPendingFunctors_ = false;
};
