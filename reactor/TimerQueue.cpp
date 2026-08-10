//
// TimerQueue 实现
//
#include "TimerQueue.h"
#include "Timer.h"
#include "Channel.h"
#include "EventLoop.h"
#include <sys/timerfd.h>
#include <unistd.h>
#include <iostream>

// timerfd 到期时内核会写入一个 8 字节计数（本次触发的次数）
// 每次读走这个计数，计数器清零，为下一次到期做准备
namespace {
const uint64_t kTimerfdReadSize = 8;
}

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop) {
    // 创建 timerfd
    // CLOCK_MONOTONIC：单调时钟（不被系统时间修改影响），配合 TFD_TIMER_ABSTIME
    // TFD_NONBLOCK：读不阻塞；TFD_CLOEXEC：exec 时自动关闭
    timerfd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd_ < 0) {
        std::cerr << "TimerQueue: timerfd_create failed" << std::endl;
        abort();
    }

    // 为 timerfd 创建 Channel，注册到 EventLoop
    // 定时器到期 → epoll 检测到 timerfd 可读 → handleRead()
    timerfdChannel_ = std::make_unique<Channel>(loop, timerfd_);
    timerfdChannel_->setReadCallback([this] { handleRead(); });
    timerfdChannel_->enableReading();
}

TimerQueue::~TimerQueue() {
    timerfdChannel_->disableAll();
    ::close(timerfd_);
    // 释放所有 Timer 对象
    for (auto& [expiration, timer] : timers_) {
        (void)expiration;
        delete timer;
    }
}

// 添加定时器（必须在 loop 线程调用，由 EventLoop::runAfter 等内部 runInLoop 保证）
void TimerQueue::addTimer(TimerCallback cb,
                          std::chrono::steady_clock::time_point when,
                          int64_t interval_ms) {
    Timer* timer = new Timer(std::move(cb), when, interval_ms, ++nextSeq_);

    // 判断新定时器是否比当前最早的还早：
    //   - 之前没有定时器，或
    //   - 新定时器到期时间早于树中最小（begin()）的
    // 是 → 必须立刻更新 timerfd，否则它要等"旧最早到期时"才被检查到
    bool earliestChanged =
        timers_.empty() || when < timers_.begin()->first;

    timers_.insert(Entry(when, timer));
    if (earliestChanged) {
        resetTimerfd();
    }
}

// timerfd 可读 —— 处理所有已到期的定时器
void TimerQueue::handleRead() {
    // 读走计数，清零 timerfd（否则会一直可读，导致忙转）
    uint64_t expirations;
    ssize_t n = ::read(timerfd_, &expirations, sizeof(expirations));
    if (n != static_cast<ssize_t>(sizeof(expirations))) {
        std::cerr << "TimerQueue::handleRead read timerfd failed" << std::endl;
        return;
    }

    // 取出所有已到期的 Timer（从 timers_ 中移除）
    std::vector<Timer*> expired = getExpired();

    // 逐个执行回调；repeat 定时器重排后重新入队
    for (Timer* timer : expired) {
        timer->run();
        if (timer->repeat()) {
            timer->restart();          // 到期时间 += interval
            timers_.insert(Entry(timer->expiration(), timer));
        } else {
            delete timer;              // 一次性定时器，直接释放
        }
    }

    // 执行完可能新增了 repeat 定时器，调整 timerfd 到下一个最早到期
    resetTimerfd();
}

// 取出所有已到期（expiration <= now）的定时器
// 遍历 timers_ 开头连续满足条件的节点，移入 expired 并从树中删除
std::vector<Timer*> TimerQueue::getExpired() {
    auto now = std::chrono::steady_clock::now();
    // timers_.begin() 是到期最早的；begin() 之后可能还有多个也到期了
    // 找到第一个"还没到期"的位置 —— 它之前的所有节点都该取出
    auto end = timers_.lower_bound(Entry(now, nullptr));

    std::vector<Timer*> expired;
    expired.reserve(std::distance(timers_.begin(), end));
    for (auto it = timers_.begin(); it != end; ++it) {
        expired.push_back(it->second);
    }
    timers_.erase(timers_.begin(), end);
    return expired;
}

// 设置 timerfd 的到期时间为最早定时器的到期时间
// timerfd 用 TFD_TIMER_ABSTIME + CLOCK_MONOTONIC，所以直接传绝对时刻的 timespec
void TimerQueue::resetTimerfd() {
    itimerspec new_value;
    new_value.it_value.tv_sec = 0;
    new_value.it_value.tv_nsec = 0;
    new_value.it_interval.tv_sec = 0;
    new_value.it_interval.tv_nsec = 0;

    if (!timers_.empty()) {
        // 只关心最早的定时器（set 的 begin()）
        timespec ts = timePointToTimespec(timers_.begin()->first);
        new_value.it_value = ts;
    }
    // timers_ 为空 → it_value 为 0，等于 disarm（取消定时）
    // 注：repeat 逻辑由 Timer::restart() + 重新入队维护，这里不设 it_interval

    if (::timerfd_settime(timerfd_, TFD_TIMER_ABSTIME, &new_value, nullptr) < 0) {
        std::cerr << "TimerQueue::resetTimerfd timerfd_settime failed" << std::endl;
    }
}

// steady_clock::time_point → timespec
// timerfd(CLOCK_MONOTONIC) + TFD_TIMER_ABSTIME 需要的 timespec 就是
// 从 monotonic epoch 开始经过的秒 + 纳秒，恰好等于 time_point 的 time_since_epoch()
timespec TimerQueue::timePointToTimespec(std::chrono::steady_clock::time_point tp) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  tp.time_since_epoch())
                  .count();
    timespec ts;
    ts.tv_sec = ns / 1000000000;
    ts.tv_nsec = ns % 1000000000;
    return ts;
}
