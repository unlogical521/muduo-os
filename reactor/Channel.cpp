//
// Channel 实现
//
#include "Channel.h"
#include "EventLoop.h"
#include <iostream>

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd), events_(0), revents_(0) {}

Channel::~Channel() {
    // 如果 events_ 不为 0，需要从 epoll 移除
    // 若 events_ 已为 0（handleClose 已调用过 disableAll），跳过
    if (!isNoneEvent()) {
        disableAll();
    }
}

void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::handleEvent() {
    // 回调函数中，需要进行双重判断
    // 1、实际发生了什么事件
    // 2、该fd关注什么事件
    if (revents_ & (EPOLLERR)) {
        if (errorCallback_) errorCallback_();
    }
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (revents_ & EPOLLRDHUP) {
            // 对方关闭连接
            if (closeCallback_) closeCallback_();
        } else {
            if (readCallback_) readCallback_();
        }
    }

    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }

    // EPOLLHUP 表示挂断（通常 fd 已关闭）
    if (revents_ & EPOLLHUP && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }
}
