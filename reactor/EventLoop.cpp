//
// EventLoop 实现
//
#include "EventLoop.h"
#include "Channel.h"
#include "TimerQueue.h"
#include "Logger.h"
#include <cassert>
#include <csignal>
#include <unistd.h>
#include <sys/eventfd.h>

// 每个线程独立 ID 计数器（用于日志）
namespace {
thread_local int t_loopCounter = 0;
}

EventLoop::EventLoop()
    : quit_(false),
      events_(kInitEventListSize),
      threadId_(std::this_thread::get_id()),
      callingPendingFunctors_(false) {

    // TCP 服务器必须忽略 SIGPIPE：向已断开/被对端 RST 的 fd 写数据时，
    // 内核会发 SIGPIPE，默认行为是终止整个进程。网络库应向调用方返回
    // EPIPE/ECONNRESET 错误码（sendInLoop 已处理），而不是让进程猝死。
    // 在第一个 EventLoop 构造时全局设置一次（进程级，重复设置无害）。
    ::signal(SIGPIPE, SIG_IGN);

    // 创建 epoll 实例
    // epoll_create1(0) 相比 epoll_create(0) 去除了 size 参数，更简洁
    epfd_ = epoll_create1(0);
    if (epfd_ < 0) {
        LOG_FATAL << "EventLoop: epoll_create1 failed";
    }

    // 创建 eventfd 用于跨线程唤醒
    // EFD_NONBLOCK: 保证 read 不阻塞；EFD_CLOEXEC: exec 时自动关闭
    // 原理：其他线程向 wakeupFd_ 写入 1 → epoll_wait 立即返回 → 消费 eventfd → 执行 pendingFunctors
    wakeupFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeupFd_ < 0) {
        LOG_FATAL << "EventLoop: eventfd failed";
    }

    // 为 wakeupFd_ 创建 Channel，加入 epoll 监听
    // 这样 epoll_wait 就能响应 eventfd 的可读事件，从而处理跨线程投递的任务
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback([this] { handleWakeup(); });
    wakeupChannel_->enableReading();

    // 创建定时器队列（内部含 timerfd Channel，注册到 epoll）
    // 此后 runAt/runAfter/runEvery 都可使用
    timerQueue_ = std::make_unique<TimerQueue>(this);

    int id = ++t_loopCounter;
    (void)id; // 留作调试使用
}

EventLoop::~EventLoop() {
    // 必须先析构 timerQueue_：它的 timerfdChannel 需要 epoll_ctl 从 epfd_ 摘除，
    // 而成员逆序析构在函数体之后，那时 epfd_ 已被 close，会触发 EBADF。
    // 这里显式 reset 保证 epfd_ 仍有效时完成清理。
    timerQueue_.reset();

    wakeupChannel_->disableAll();
    removeChannel(wakeupChannel_.get());
    ::close(wakeupFd_);
    ::close(epfd_);
}

// 主事件循环 — 多路事件分发器
// 循环逻辑：
//   1. epoll_wait 阻塞等待事件（或 wakeup 打断）
//   2. 逐个分发就绪 fd 到对应 Channel::handleEvent()
//   3. 如果事件数达到容量则扩容（减少下次 epoll_wait 的 realloc）
//   4. 执行跨线程投递的待办回调 doPendingFunctors()
// 为什么第 4 步放在这里而不是每次 handleEvent 之后？
//   - 因为多个 pending functor 间往往没有依赖，批量执行减少锁竞争
void EventLoop::loop() {
    quit_ = false;
    while (!quit_) {
        // 1. 等待事件（-1 表示无限超时，直到有事件或 wakeup 发生）
        int nfds = epoll_wait(epfd_, events_.data(),
                              static_cast<int>(events_.size()), -1);
        if (nfds < 0) {
            if (errno == EINTR) {
                // 被信号打断（如 SIGTERM 优雅退出）：信号处理器已置 quit_。
                // 这里不能直接 continue 退出 —— 若 quit_ 已置位，while 条件会立即退出，
                // 而 doPendingFunctors 还没执行，已投递的回调（如 Server 析构时 closeNow
                // 调度的连接摘除）会被丢弃，导致连接在 loop 销毁后仍注册事件 → 析构崩溃。
                // 因此 EINTR 时也要先执行一次 pending functors 再回到 while 检查退出。
                doPendingFunctors();
                continue;
            }
            LOG_ERROR << "EventLoop: epoll_wait failed, errno=" << errno;
            break;
        }

        // 2. 分发事件到各个 Channel
        // 每个 events_[i].data.ptr 在 updateChannel 时已被设为 Channel*
        // 所以这里能直接拿到对应的 Channel 对象，无需查找
        for (int i = 0; i < nfds; ++i) {
            Channel* ch = static_cast<Channel*>(events_[i].data.ptr);
            ch->setRevents(events_[i].events);
            ch->handleEvent();
        }

        // 3. 如果返回事件数达到容量，扩容（避免频繁调整）
        if (nfds == static_cast<int>(events_.size())) {
            events_.resize(events_.size() * 2);
        }

        // 4. 执行跨线程投递的待办回调
        doPendingFunctors();
    }

    // 退出前最后一次清空 pending 队列：
    // 竞态 —— 其它线程可能恰好在"最后一个 doPendingFunctors 已执行完、但 while 条件
    // 即将判断 quit_ 退出"的间隙投递回调（如 Server 析构时 closeNow 调度的连接摘除）。
    // 若直接返回，这些回调被丢弃，连接会在 loop 销毁后仍带事件注册 → 析构崩溃。
    // 这里再清空一次，保证退出前所有已投递回调都已执行。
    doPendingFunctors();
}

// ========== Channel 管理 ==========

// 更新一个 Channel 在 epoll 中的注册状态
// 根据 Channel::events_ 的值决定是 ADD、MOD 还是 DEL：
//   - 不在 channels_ 中 → EPOLL_CTL_ADD，加入映射
//   - 在 channels_ 中且 events_ == 0 → EPOLL_CTL_DEL，移除映射
//   - 在 channels_ 中且 events_ != 0 → EPOLL_CTL_MOD，更新事件
void EventLoop::updateChannel(Channel* ch) {
    int fd = ch->fd();
    auto it = channels_.find(fd);

    epoll_event ev;
    ev.events   = ch->events();
    ev.data.ptr = ch;   // 使用 ptr 而不是 fd，方便 Channel 派发
    // 加入新的
    if (it == channels_.end()) {
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERROR << "EventLoop: epoll_ctl ADD failed fd=" << fd;
            return;
        }
        channels_[fd] = ch;
    }
    // 该fd没有关注的事件，通信已结束
    else if (ch->isNoneEvent()) {
        if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            LOG_ERROR << "EventLoop: epoll_ctl DEL failed fd=" << fd;
        }
        channels_.erase(fd);
    }
    // 旧的fd更新关注事件
    else {
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            LOG_ERROR << "EventLoop: epoll_ctl MOD failed fd=" << fd;
        }
    }
}

// 移除 Channel（直接删除，不经过 disableAll 流程）
void EventLoop::removeChannel(Channel* ch) {
    int fd = ch->fd();
    auto it = channels_.find(fd);
    if (it != channels_.end()) {
        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
        channels_.erase(it);
    }
}

// ========== 跨线程调度 ==========

// 在 loop 线程上安全执行回调
// 如果当前线程就是 loop 所属线程，直接同步执行；
// 否则将回调入队到 pendingFunctors_，并唤醒 loop 线程异步执行
void EventLoop::runInLoop(Functor cb) {
    if (isInLoopThread()) {
        cb();  // 本线程 → 直接执行（同步，零开销）
    } else {
        queueInLoop(std::move(cb));  // 跨线程 → 入队 + 唤醒
    }
}

// 将回调加入待办队列，唤醒 loop 线程
// 唤醒条件：不在本 loop 线程，或者在 doPendingFunctors 执行过程中
//   为什么在 callingPendingFunctors_ 时也唤醒？
//   → 考虑以下竞态：doPendingFunctors 已将所有 functor 取出（pendingFunctors_ 为空），
//     但还没执行到正在调用的那一个。此时另一个线程入队了一个新 functor 并执行了 wakeup，
//     但 wakeupFd_ 已经被读取过了（上一轮 handleWakeup），
//     新 functor 要等到下一次 epoll_wait 返回才会执行。
//     加上 callingPendingFunctors_ 的判断，能在当前 doPendingFunctors 的最后再执行一次。
void EventLoop::queueInLoop(Functor cb) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }

    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
}

// 写 eventfd 唤醒 loop 线程
// 原理：eventfd 是一个内核计数器，write 写入 1 使计数器 +1，
// epoll 会检测到 wakeupFd_ 可读，从而从 epoll_wait 返回
void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    (void)n;
}

// 消费 eventfd 的唤醒事件
// 读走 wakeupFd_ 的值，将计数器归零，为下一次 wakeup 做准备
void EventLoop::handleWakeup() {
    uint64_t one;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    (void)n;
}

// 执行所有跨线程投递的待办回调
// 设计要点：
//   - 将 pendingFunctors_ 整体 swap 到局部临时变量，减少锁持有时间
//   - 执行期间不持有锁，允许其他线程向 pendingFunctors_ 继续追加
//   - callingPendingFunctors_ 标志确保 queueInLoop 能在执行期间正确唤醒
void EventLoop::doPendingFunctors() {
    std::vector<Functor> functors;
    callingPendingFunctors_ = true;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        functors.swap(pendingFunctors_);
    }

    for (auto& f : functors) {
        f();
    }

    callingPendingFunctors_ = false;
}

// ========== 定时器 ==========

// 定时器注册都通过 runInLoop 调度到 loop 线程，
// 保证 TimerQueue::addTimer 只在 loop 线程执行（无需加锁）

void EventLoop::runAt(std::chrono::steady_clock::time_point when, TimerCallback cb) {
    runInLoop([this, when, cb = std::move(cb)]() {
        timerQueue_->addTimer(std::move(cb), when, 0);
    });
}

void EventLoop::runAfter(std::chrono::milliseconds delay, TimerCallback cb) {
    runInLoop([this, delay, cb = std::move(cb)]() {
        timerQueue_->addTimer(std::move(cb),
                              std::chrono::steady_clock::now() + delay, 0);
    });
}

void EventLoop::runEvery(std::chrono::milliseconds interval, TimerCallback cb) {
    runInLoop([this, interval, cb = std::move(cb)]() {
        timerQueue_->addTimer(std::move(cb),
                              std::chrono::steady_clock::now() + interval,
                              interval.count());
    });
}
