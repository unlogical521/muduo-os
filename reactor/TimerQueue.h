//
// TimerQueue — 定时器队列（仿 muduo）
//
// 核心思想：把定时器变成一个 fd（timerfd）注册进 epoll，
// 定时器到期时 epoll_wait 返回 timerfd 可读，触发 handleRead，
// 从而与网络 IO 统一在一个事件循环里，无需额外线程、无锁。
//
// 数据结构：std::set（红黑树），按到期时间排序。
//   - 增删查都是 O(logN)
//   - 每次只关心"最早到期"的定时器（set 的 begin()），用来设置 timerfd
//
// 工作流程：
//   1. addTimer() 插入新 Timer 到 timers_
//      若它比当前最早的还早 → resetTimerfd() 更新 timerfd 到期时间
//   2. timerfd 到期可读 → handleRead()
//      读走计数 → getExpired() 取出所有已到期 Timer → 逐个 run()
//      repeat 定时器 restart() 后重插 → 最后 resetTimerfd() 调整到下一个最早
//
// 线程安全：所有方法都在所属 EventLoop 线程上调用（EventLoop::runAfter
//          内部通过 runInLoop 保证），无需加锁。
//
// 备注：本实现不含 cancel()（取消定时器）。如需取消，参照 muduo 增加
//      TimerId + activeTimers_/cancellingTimers_ 两套集合即可。
//
#pragma once

#include <chrono>
#include <ctime>
#include <functional>
#include <memory>
#include <set>
#include <vector>

class EventLoop;
class Channel;
class Timer;

class TimerQueue {
public:
    using TimerCallback = std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    // 禁止拷贝
    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    // 添加一个定时器（必须在 loop 线程调用）
    // when：到期绝对时间（单调时钟）；interval_ms：重复间隔（毫秒），0 = 一次性
    void addTimer(TimerCallback cb,
                  std::chrono::steady_clock::time_point when,
                  int64_t interval_ms);

private:
    // 红黑树节点：按 (到期时间, 序号) 排序
    // 用 pair 而不是只比到期时间，保证两个 Timer 同刻到期时也能区分先后、不冲突
    using Entry = std::pair<std::chrono::steady_clock::time_point, Timer*>;

    // timerfd 可读时的处理（epoll 触发）
    void handleRead();
    // 取出现在为止所有已到期的定时器（从 timers_ 移到 expired，释放树节点）
    std::vector<Timer*> getExpired();
    // 设置 timerfd 的到期时间为第一个定时器的到期时间（无定时器则 disarm）
    void resetTimerfd();
    // 将 steady_clock::time_point 转为 timespec（供 timerfd_settime 使用）
    static timespec timePointToTimespec(std::chrono::steady_clock::time_point tp);

    EventLoop* loop_;
    int timerfd_;                     // timerfd 文件描述符
    std::unique_ptr<Channel> timerfdChannel_;  // timerfd 的 Channel（注册到 epoll）
    std::set<Entry> timers_;          // 所有定时器（按到期时间排序）
    int64_t nextSeq_{0};              // 全局递增序号
};
