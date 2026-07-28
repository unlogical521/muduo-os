//
// EventLoopThreadPool — IO 线程池
//
// 管理一组 EventLoopThread，提供 round-robin 方式获取下一个
// EventLoop，用于将新连接分发给不同的 sub-reactor。
//
// 设计：
//   - baseLoop_（main reactor）处理 accept
//   - threads_ 中的 loop（sub reactor）处理已建立连接的 IO
//   - getNextLoop() 轮询分发
//
#pragma once

#include <memory>
#include <vector>
#include <functional>
#include <string>

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    explicit EventLoopThreadPool(EventLoop* baseLoop,
                                 std::string name = std::string());
    ~EventLoopThreadPool();

    // 禁止拷贝
    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    // 设置 IO 线程数量（必须在 start() 前调用）
    void setThreadNum(int num) { threadNum_ = num; }

    // 启动线程池，创建 threadNum_ 个 EventLoopThread
    void start();

    // 轮询获取下一个 sub-reactor 的 EventLoop
    EventLoop* getNextLoop();

    // 返回所有 sub-reactor 的 EventLoop 列表
    std::vector<EventLoop*> getAllLoops() const;

    // 返回主 Reactor 的 EventLoop
    EventLoop* getBaseLoop() const { return baseLoop_; }

    bool started() const { return started_; }

private:
    EventLoop* baseLoop_;                              // 主 Reactor（accept 专用）
    std::string name_;
    bool started_;
    int threadNum_;
    int next_;                                         // 轮询游标

    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;                    // sub-reactor 的 EventLoop 列表
};
