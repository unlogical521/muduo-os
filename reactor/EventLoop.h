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
// 线程安全契约：
//   - loop() 只允许在创建该 EventLoop 的线程上调用（即 threadId_ 线程）
//   - runInLoop/queueInLoop 可在任意线程调用
//   - updateChannel/removeChannel 只应在 loop_ 线程调用（Channel 的 enable/disable 内部调用它们）
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
#include <chrono>

class Channel;
class TimerQueue;

class EventLoop {
public:
    using Functor = std::function<void()>;
    using TimerCallback = std::function<void()>;

    EventLoop();
    ~EventLoop();

    // 禁止拷贝
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // 启动事件循环（阻塞，直到 quit() 被调用）
    // 必须在创建该 EventLoop 的线程上调用
    void loop();

    // 退出事件循环（线程安全）
    // 可在任意线程调用：设置 quit_ 标志后立即 wakeup 以打断 epoll_wait
    // 主循环检测 quit_标志 退出
    // 
    void quit() { quit_ = true; wakeup(); }

    // 更新/移除 Channel 在 epoll 中的注册状态
    // 由 Channel::update() 调用，必须在 loop_ 线程执行
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

    // === 定时器 ===
    // 三者内部都走 runInLoop，保证在 loop 线程安全注册。
    // 回调会在 EventLoop 线程上执行，不要在回调里做重活。

    // 在指定绝对时间（单调时钟）执行一次回调
    void runAt(std::chrono::steady_clock::time_point when, TimerCallback cb);
    // 延迟 delay 毫秒后执行一次
    void runAfter(std::chrono::milliseconds delay, TimerCallback cb);
    // 每 interval 毫秒重复执行
    void runEvery(std::chrono::milliseconds interval, TimerCallback cb);

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

    // 定时器支持（声明在最后：析构时逆序先于 wakeupChannel_，而 loop_ 仍有效）
    std::unique_ptr<TimerQueue> timerQueue_;
};
