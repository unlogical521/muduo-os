//
// Timer — 一个定时器（由 TimerQueue 管理）
//
// 职责：保存一个到期时间 + 一个回调，到点后执行。
// 支持两种模式：
//   - 一次性定时器：interval_ == 0，执行一次后不再重排
//   - 重复定时器：  interval_ > 0，restart() 时到期时间 += interval，循环执行
//
// 时钟：统一使用 steady_clock（单调时钟，不受系统时间调整影响），
//       与 TimerQueue 的 timerfd(CLOCK_MONOTONIC) 保持一致。
//
// 线程安全：Timer 全部在所属 EventLoop 线程上操作（addTimer / handleRead 触发），
//           无需加锁。
//
#pragma once

#include <chrono>
#include <functional>

class Timer {
public:
    using TimerCallback = std::function<void()>;

    // when：到期绝对时间（单调时钟）
    // interval_ms：重复间隔（毫秒），0 表示一次性定时器
    // sequence：全局递增序号，用于在到期时间相同时区分先后（std::set 排序需要）
    Timer(TimerCallback cb,
          std::chrono::steady_clock::time_point when,
          int64_t interval_ms,
          int64_t sequence);

    // 到点执行回调
    void run() const {
        if (callback_) {
            callback_();
        }
    }

    // 重新安排下一次到期时间（仅 repeat 定时器）
    // interval_ > 0 时：expiration_ += interval，重新进入队列
    void restart();

    // getter
    std::chrono::steady_clock::time_point expiration() const { return expiration_; }
    bool repeat() const { return repeat_; }
    int64_t sequence() const { return sequence_; }

private:
    TimerCallback callback_;
    std::chrono::steady_clock::time_point expiration_;  // 到期绝对时间（单调时钟）
    int64_t interval_ms_;                               // 重复间隔（毫秒），0 = 一次性
    bool repeat_;                                       // 是否为重复定时器
    int64_t sequence_;                                  // 创建序号（同刻到期时排序用）
};
