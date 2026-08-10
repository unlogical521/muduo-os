//
// EventLoopThreadPool — IO 线程池
//
// 管理一组 EventLoopThread，提供 round-robin 方式获取下一个
// EventLoop，用于将新连接分发给不同的 sub-reactor。
//
// 设计（多 Reactor 模式）：
//   - baseLoop_（main reactor）处理 accept
//   - threads_ 中的 loop（sub reactor）处理已建立连接的 IO
//   - getNextLoop() 轮询分发，连接数均匀散列到各 sub-reactor
//
// 使用方式：
//   pool.setThreadNum(4);
//   pool.start();           // 创建 4 个 EventLoopThread
//   EventLoop* io = pool.getNextLoop();  // 轮询选一个
//   io->runInLoop(task);                 // 投递到 sub-reactor
//
// 当 threadNum_ == 0 时，退化为单线程模型（baseLoop_ 自己处理所有）。
// 这也是一个合法状态——不需要为低负载场景额外开线程。
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

    // 禁止拷贝（线程池不可复制）
    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    // 设置 IO 线程数量（必须在 start() 前调用）
    void setThreadNum(int num) { threadNum_ = num; }

    // 启动线程池，创建 threadNum_ 个 EventLoopThread
    // 每个 EventLoopThread 内部会创建自己的 EventLoop
    void start();

    // 轮询获取下一个 sub-reactor 的 EventLoop
    // 使用 round-robin 实现简单负载均衡
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
    int next_;                                         // round-robin 游标

    std::vector<std::unique_ptr<EventLoopThread>> threads_;  // 线程池
    std::vector<EventLoop*> loops_;                         // sub-reactor 的 EventLoop 指针列表
};
