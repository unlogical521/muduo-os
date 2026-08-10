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

// 析构函数：退出事件循环 + 等待线程结束
// 先 quit 让 loop.loop 返回，再 join 等待 threadFunc 彻底退出
EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_) {
        loop_->quit();     // 设置 quit_ = true，打断 epoll_wait
        thread_.join();    // 等待线程结束
    }
}

// startLoop — 启动线程并等待 EventLoop 就绪
// 典型的多 Reactor 启动流程：
//   1. 创建 std::thread 运行 threadFunc
//   2. 用 condition_variable 等待 threadFunc 中 EventLoop 构造完成
//   3. 返回 EventLoop* 给调用方（EventLoopThreadPool）
EventLoop* EventLoopThread::startLoop() {
    // 启动线程（threadFunc 会创建栈上的 EventLoop）
    // thread_---loop,one thread per loop;
    thread_ = std::thread(&EventLoopThread::threadFunc, this);

    EventLoop* loop = nullptr;
    {
        // 等待子线程完成 EventLoop 的创建
        // 这里不能用简单的 sleep，因为创建 EventLoop 的时间不确定
        std::unique_lock<std::mutex> lock(mutex_);
        // 条件变量，等待loop创建
        cond_.wait(lock, [this] { return loop_ != nullptr; });
        loop = loop_;
    }

    return loop;
}

// threadFunc — 线程执行函数（在独立的 std::thread 上运行）
// 执行流程：
//   1. 在栈上创建 EventLoop（栈上的对象生命周期随本函数）
//   2. 调用回调（用于日志或设置）
//   3. 通知 startLoop() 线程 loop 已就绪
//   4. 进入事件循环（阻塞，直到 loop.quit() 被调用）
void EventLoopThread::threadFunc() {
    EventLoop loop;                 // 栈上分配 EventLoop（含 epoll fd、wakeup fd）

    if (callback_) {
        callback_(&loop);           // 线程初始化回调（用于日志打印线程名）
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond_.notify_one();         // 通知 startLoop() 可以返回了
    }

    loop.loop();                    // 进入事件循环（阻塞）

    // loop 退出后清空指针，指示析构函数不要重复 quit
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}
