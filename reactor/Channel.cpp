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
    // 若 events_ 已为 0（handleClose 已调用过 disableAll，事件已注销），跳过
    if (!isNoneEvent()) {
        disableAll();
    }
}

// 通知所属 EventLoop 更新 epoll 注册状态
// 由 enable/disable 系列函数内部调用，最终走到 EventLoop::updateChannel
void Channel::update() {
    loop_->updateChannel(this);
}

// 事件分发核心函数
// 根据 epoll_wait 填充的 revents_ 判断发生了什么事件，调用对应回调
//
// 事件类型优先级：
//   1. EPOLLERR     → 错误（如 fd 上发生了异步错误）
//   2. EPOLLIN      → 可读（普通数据到达）
//   3. EPOLLRDHUP   → 对方关闭连接（比 EPOLLHUP 更精细，能区分"关闭"和"挂断"）
//   4. EPOLLOUT     → 可写（发送缓冲区有空闲）
//   5. EPOLLHUP     → 挂断（通常表示 fd 已无意义，如 pipe 读端关闭）
//
// 注意：一个 fd 可能同时有多个事件（如 EPOLLIN | EPOLLOUT），
// 所以这里用 if（而不是 else if）逐个检查
void Channel::handleEvent() {
    // EPOLLERR：当 fd 上发生错误时 epoll 会自动设置此标志
    // 常见场景：非阻塞 connect 失败，或 SO_LINGER 关闭后仍有数据
    if (revents_ & (EPOLLERR)) {
        if (errorCallback_) errorCallback_();
    }

    // EPOLLIN / EPOLLPRI / EPOLLRDHUP：读相关事件
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (revents_ & EPOLLRDHUP) {
            // EPOLLRDHUP 是 Linux 2.6.17+ 的特性：对端关闭连接或 shutdown(SHUT_WR)
            // 比 EPOLLHUP 更精确——它保证还能读走剩余数据
            if (closeCallback_) closeCallback_();
        } else {
            // 正常可读事件
            if (readCallback_) readCallback_();
        }
    }

    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }

    // EPOLLHUP：对端挂断，但此时通常没有 EPOLLIN 标志（读不到数据了）
    // 需要 && !(EPOLLIN) 避免与正常的关闭流程重复处理
    if (revents_ & EPOLLHUP && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }
}
