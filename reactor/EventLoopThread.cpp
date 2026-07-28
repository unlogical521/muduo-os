//
// EventLoopThread 实现
//
#include "EventLoopThread.h"
#include "EventLoop.h"
#include <iostream>

EventLoopThread::EventLoopThread(ThreadInitCallback cb,
                                 std::string name)
    : loop_(nullptr),
      callback_(std::move(cb)),
      name_(std::move(name)),
      exiting_(false) {}

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_) {
        loop_->quit();
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    thread_ = std::thread(&EventLoopThread::threadFunc, this);

    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return loop_ != nullptr; });
        loop = loop_;
    }

    return loop;
}

void EventLoopThread::threadFunc() {
    EventLoop loop;

    if (callback_) {
        callback_(&loop);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond_.notify_one();
    }

    loop.loop();

    // loop 退出后清空指针
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}
