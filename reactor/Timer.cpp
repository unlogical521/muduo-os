//
// Timer 实现
//
#include "Timer.h"

Timer::Timer(TimerCallback cb,
             std::chrono::steady_clock::time_point when,
             int64_t interval_ms,
             int64_t sequence)
    : callback_(std::move(cb)),
      expiration_(when),
      interval_ms_(interval_ms),
      repeat_(interval_ms > 0),
      sequence_(sequence) {}

void Timer::restart() {
    // 只有 repeat 定时器才允许 restart
    if (repeat_) {
        expiration_ += std::chrono::milliseconds(interval_ms_);
    }
}
