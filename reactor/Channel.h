//
// Channel — 封装一个 fd，注册感兴趣的事件和对应的回调
//
// 每个 Channel 只属于一个 EventLoop。
// EventLoop 通过 Channel 知道 fd 上发生了什么事，然后调用对应的回调。
//
// 状态机：
//   events_ 表示"这个 Channel 对哪些事件感兴趣"
//   revents_ 表示"本次 epoll_wait 返回后这个 fd 实际发生了什么"
//   handleEvent() 根据 revents_ 判断事件类型，调用对应的回调
//
// 线程安全：
//   - 所有 Channel 的操作都应在所属 EventLoop 线程上调用
//   - enable/disable 系列函数会通过 update() 触发 epoll_ctl，非线程安全
//
#pragma once

#include <sys/epoll.h>
#include <functional>
#include <memory>

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;
    // 构造
    Channel(EventLoop* loop, int fd);
    // 析构
    ~Channel();

    // 禁止拷贝（Channel 有明确的所属关系）
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    int fd() const { return fd_; }
    uint32_t events() const { return events_; }

    // === 事件注册（通过 update() 最终调用 epoll_ctl）===

    // 关注读事件
    void enableReading()  { events_ |= EPOLLIN;  update(); }
    // 关注写事件
    void enableWriting()  { events_ |= EPOLLOUT; update(); }
    // 取消关注读事件
    void disableReading() { events_ &= ~EPOLLIN;  update(); }
    // 取消关注写事件
    void disableWriting() { events_ &= ~EPOLLOUT; update(); }
    // 取消关注所有事件（通常用于连接关闭前）
    void disableAll()     { events_ = 0;          update(); }

    // 由 EventLoop 在 epoll_wait 返回后设置实际发生的事件
    void setRevents(uint32_t revents) { revents_ = revents; }

    // 由 EventLoop 在事件循环中调用 —— 根据 revents_ 分发到具体回调
    void handleEvent();

    // === 回调注册 ===
    // 实际处理呢？
    void setReadCallback(EventCallback cb)  { readCallback_  = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

    // 标记本 Channel 不再关注任何事件（用于析构前检查）
    bool isNoneEvent() const { return events_ == 0; }

private:
    // 通知 EventLoop 更新 epoll 注册状态（最终调用 epoll_ctl ADD/MOD/DEL）
    void update();

    EventLoop* loop_;
    int fd_;

    uint32_t events_;   // 感兴趣的事件（由 enable/disable 设置，是 epoll 的过滤条件）
    uint32_t revents_;  // 实际发生的事件（由 epoll_wait 填充，handleEvent 判断的依据）

    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;
};
