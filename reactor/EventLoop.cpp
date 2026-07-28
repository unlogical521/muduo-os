//
// EventLoop 实现
//
#include "EventLoop.h"
#include "Channel.h"
#include <iostream>
#include <cassert>
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

    // 创建 epoll 实例
    epfd_ = epoll_create1(0);
    if (epfd_ < 0) {
        std::cerr << "EventLoop: epoll_create1 failed" << std::endl;
        abort();
    }

    // 创建 eventfd 用于跨线程唤醒
    wakeupFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeupFd_ < 0) {
        std::cerr << "EventLoop: eventfd failed" << std::endl;
        abort();
    }

    // 为 wakeupFd_ 创建 Channel，加入 epoll 监听
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback([this] { handleWakeup(); });
    wakeupChannel_->enableReading();

    int id = ++t_loopCounter;
    (void)id; // 留作调试使用
}

EventLoop::~EventLoop() {
    wakeupChannel_->disableAll();
    removeChannel(wakeupChannel_.get());
    ::close(wakeupFd_);
    ::close(epfd_);
}

void EventLoop::loop() {
    quit_ = false;
    while (!quit_) {
        // 1. 等待事件
        int nfds = epoll_wait(epfd_, events_.data(),
                              static_cast<int>(events_.size()), -1);
        if (nfds < 0) {
            std::cerr << "EventLoop: epoll_wait failed" << std::endl;
            break;
        }

        // 2. 分发事件到各个 Channel
        for (int i = 0; i < nfds; ++i) {
            Channel* ch = static_cast<Channel*>(events_[i].data.ptr);
            ch->setRevents(events_[i].events);
            ch->handleEvent();
        }

        // 3. 如果返回事件数达到容量，扩容
        if (nfds == static_cast<int>(events_.size())) {
            events_.resize(events_.size() * 2);
        }

        // 4. 执行跨线程投递的待办回调
        doPendingFunctors();
    }
}

// ========== Channel 管理 ==========

void EventLoop::updateChannel(Channel* ch) {
    int fd = ch->fd();
    auto it = channels_.find(fd);

    epoll_event ev;
    ev.events   = ch->events();
    ev.data.ptr = ch;

    if (it == channels_.end()) {
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::cerr << "EventLoop: epoll_ctl ADD failed fd=" << fd << std::endl;
            return;
        }
        channels_[fd] = ch;
    } else if (ch->isNoneEvent()) {
        if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            std::cerr << "EventLoop: epoll_ctl DEL failed fd=" << fd << std::endl;
        }
        channels_.erase(fd);
    } else {
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            std::cerr << "EventLoop: epoll_ctl MOD failed fd=" << fd << std::endl;
        }
    }
}

void EventLoop::removeChannel(Channel* ch) {
    int fd = ch->fd();
    auto it = channels_.find(fd);
    if (it != channels_.end()) {
        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
        channels_.erase(it);
    }
}

// ========== 跨线程调度 ==========

void EventLoop::runInLoop(Functor cb) {
    if (isInLoopThread()) {
        cb();  // 本线程 → 直接执行
    } else {
        queueInLoop(std::move(cb));  // 跨线程 → 入队
    }
}

void EventLoop::queueInLoop(Functor cb) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }

    // 如果不在本 loop 线程，或者正在执行 pending functors（可能刚执行完一批，
    // 但新入队的还没被执行，需要再唤醒一次）
    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    (void)n;
}

void EventLoop::handleWakeup() {
    uint64_t one;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    (void)n;
}

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
