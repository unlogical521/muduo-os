//
// EventLoopThreadPool 实现
//
#include "EventLoopThreadPool.h"
#include "EventLoopThread.h"
#include "EventLoop.h"
#include <iostream>

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop,
                                         std::string name)
    : baseLoop_(baseLoop),
      name_(std::move(name)),
      started_(false),
      threadNum_(0),
      next_(0) {}

EventLoopThreadPool::~EventLoopThreadPool() {
    // EventLoopThread 的 unique_ptr 析构时自动触发 quit + join
    // 无需额外清理
}

// 启动线程池
// 创建 threadNum_ 个 EventLoopThread，每个线程运行自己的 EventLoop
// 当 threadNum_ == 0 时，退化为单线程模式：
//   baseLoop_ 同时承担 accept 和 IO 两种角色
void EventLoopThreadPool::start() {
    started_ = true;

    for (int i = 0; i < threadNum_; ++i) {
        std::string threadName = name_ + "-io-" + std::to_string(i);

        // 创建 EventLoopThread，传递线程初始化回调（用于打印线程启动日志）
        auto t = std::make_unique<EventLoopThread>(
            [threadName](EventLoop* loop) {
                std::cout << "[thread] " << threadName << " started" << std::endl;
                (void)loop;
            },
            threadName);

        // startLoop 会阻塞等待，直到线程内的 EventLoop 完全就绪
        loops_.push_back(t->startLoop());
        threads_.push_back(std::move(t));
    }

    if (threadNum_ == 0) {
        // 没有 IO 线程，所有连接在主 reactor 处理（退化到单线程模型）
        std::cout << "[thread] no IO threads, using main reactor only" << std::endl;
        loops_.push_back(baseLoop_);
    }
}

// getNextLoop — round-robin 轮询获取下一个 EventLoop
// 当 threadNum_ == 0 时，loops_ 中只有 baseLoop_，每次都返回同一个
// 当 threadNum_ > 0 时，新连接依次分配到 sub-reactor[0], [1], [2], [0], ...
// 这是最简单的负载均衡策略：
//   优点：实现简单，分配均匀
//   缺点：不考虑每个 sub-reactor 当前的连接数或负载
EventLoop* EventLoopThreadPool::getNextLoop() {
    EventLoop* loop = baseLoop_;

    if (!loops_.empty()) {
        loop = loops_[next_];
        ++next_;
        if (next_ >= static_cast<int>(loops_.size())) {
            next_ = 0;
        }
    }

    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops() const {
    return loops_;
}
