//
// Channel — 封装一个 fd，注册感兴趣的事件和对应的回调
//
// 每个 Channel 只属于一个 EventLoop。
// EventLoop 通过 Channel 知道 fd 上发生了什么事，然后调用对应的回调。
//
#pragma once

#include <sys/epoll.h>
#include <functional>
#include <memory>

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    // 禁止拷贝
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    int fd() const { return fd_; }
    uint32_t events() const { return events_; }

    // 设置感兴趣的事件（给 EventLoop/Server 用，会触发 epoll_ctl 更新）
    void enableReading()  { events_ |= EPOLLIN;  update(); }
    void enableWriting()  { events_ |= EPOLLOUT; update(); }
    void disableReading() { events_ &= ~EPOLLIN;  update(); }
    void disableWriting() { events_ &= ~EPOLLOUT; update(); }
    void disableAll()     { events_ = 0;          update(); }

    // 由 EventLoop 设置本次 epoll_wait 返回后实际发生的事件
    void setRevents(uint32_t revents) { revents_ = revents; }

    // 由 EventLoop 在事件循环中调用 —— 根据 revents_ 分发到具体回调
    void handleEvent();

    // 设置回调
    void setReadCallback(EventCallback cb)  { readCallback_  = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

    // 标记本 Channel 已被移除
    bool isNoneEvent() const { return events_ == 0; }

private:
    void update();   // 通知 EventLoop 更新 epoll 注册状态

    EventLoop* loop_;
    int fd_;

    uint32_t events_;   // 感兴趣的事件
    uint32_t revents_;  // 实际发生的事件（由 epoll_wait 填充）

    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;
};
