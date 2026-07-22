//
// EventLoop — 事件循环（Reactor 的核心）
//
// 封装 epoll：
//   1. epoll_create
//   2. epoll_ctl   — 通过 Channel 的 update / remove
//   3. epoll_wait  — 在主循环中调用
//
// 运行逻辑：
//   while (!quit_) {
//       active_channels_ = epoll_wait(...)
//       for each channel in active_channels_:
//           channel->handleEvent()
//   }
//
#pragma once

#include <sys/epoll.h>
#include <vector>
#include <map>
#include <memory>

class Channel;

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    // 禁止拷贝
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // 启动事件循环（阻塞）
    void loop();

    // 退出事件循环
    void quit() { quit_ = true; }

    // 更新/移除 Channel 在 epoll 中的注册状态
    void updateChannel(Channel* ch);
    void removeChannel(Channel* ch);

    // 获取 epoll fd（给 Channel 等内部使用）
    int epfd() const { return epfd_; }

private:
    static const int kInitEventListSize = 64;

    bool quit_;
    int epfd_;                                  // epoll 实例 fd
    std::vector<epoll_event> events_;           // epoll_wait 返回的事件数组
    std::map<int, Channel*> channels_;          // fd -> Channel 映射
};
