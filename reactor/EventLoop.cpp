//
// EventLoop 实现
//
#include "EventLoop.h"
#include "Channel.h"
#include <iostream>
#include <cassert>
#include <unistd.h>

EventLoop::EventLoop()
    : quit_(false), events_(kInitEventListSize) {
    epfd_ = epoll_create1(0);
    if (epfd_ < 0) {
        std::cerr << "EventLoop: epoll_create1 failed" << std::endl;
        abort();
    }
}

EventLoop::~EventLoop() {
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

        // 2. 填充每个 Channel 的实际事件并调用 handleEvent
        for (int i = 0; i < nfds; ++i) {
            Channel* ch = static_cast<Channel*>(events_[i].data.ptr);
            ch->setRevents(events_[i].events);
            ch->handleEvent();
        }

        // 3. 如果返回的事件数达到容量，扩容（避免频繁调整）
        if (nfds == static_cast<int>(events_.size())) {
            events_.resize(events_.size() * 2);
        }
    }
}

void EventLoop::updateChannel(Channel* ch) {
    int fd = ch->fd();
    auto it = channels_.find(fd);

    epoll_event ev;
    ev.events   = ch->events();
    ev.data.ptr = ch;   // 使用 ptr 而不是 fd，方便 Channel 派发

    if (it == channels_.end()) {
        // 新 Channel，加入 epoll
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::cerr << "EventLoop: epoll_ctl ADD failed fd=" << fd << std::endl;
            return;
        }
        channels_[fd] = ch;
    } else if (ch->isNoneEvent()) {
        // events == 0，从 epoll 移除
        if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            std::cerr << "EventLoop: epoll_ctl DEL failed fd=" << fd << std::endl;
        }
        channels_.erase(fd);
    } else {
        // 修改事件
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
