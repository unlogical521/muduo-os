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
    // EventLoopThread 析构时自动退出线程
}

void EventLoopThreadPool::start() {
    started_ = true;

    for (int i = 0; i < threadNum_; ++i) {
        std::string threadName = name_ + "-io-" + std::to_string(i);

        auto t = std::make_unique<EventLoopThread>(
            [threadName](EventLoop* loop) {
                std::cout << "[thread] " << threadName << " started" << std::endl;
                (void)loop;
            },
            threadName);

        loops_.push_back(t->startLoop());
        threads_.push_back(std::move(t));
    }

    if (threadNum_ == 0) {
        // 没有 IO 线程，所有连接在主 reactor 处理（退化到单线程模型）
        std::cout << "[thread] no IO threads, using main reactor only" << std::endl;
        loops_.push_back(baseLoop_);
    }
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    EventLoop* loop = baseLoop_;

    if (!loops_.empty()) {
        // round-robin 轮询
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
