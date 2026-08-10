//
// Server 实现（多 Reactor 版本）
//
#include "Server.h"
#include "EventLoop.h"
#include "Acceptor.h"
#include "TcpConnection.h"
#include "Buffer.h"
#include "EventLoopThreadPool.h"
#include <iostream>

// 构造函数
// 1. 创建 Acceptor（注册在主 Reactor 上）
// 2. 创建 EventLoopThreadPool
Server::Server(EventLoop* loop, int port)
    : loop_(loop) {
    acceptor_ = std::make_unique<Acceptor>(loop, port);
    acceptor_->setNewConnectionCallback(
        [this](int connfd, const sockaddr_in& addr) {
            onNewConnection(connfd, addr);
        });

    threadPool_ = std::make_unique<EventLoopThreadPool>(loop, "reactor");
}

Server::~Server() = default;

void Server::setThreadNum(int num) {
    ioThreadCount_ = num;
    threadPool_->setThreadNum(num);
}

void Server::setHeartbeat(std::chrono::milliseconds timeout,
                          std::chrono::milliseconds checkInterval) {
    heartbeatTimeout_ = timeout;
    heartbeatInterval_ = checkInterval;
}

// start — 启动服务器
// 执行顺序：
//   1. threadPool_->start() — 启动所有 sub-reactor 线程
//   2. acceptor_->listen()  — 开始监听（将 listen fd 注册到主 Reactor）
//   3. （可选）注册心跳扫描定时器到 main reactor
// 这个顺序是重要的：sub-reactor 必须先就绪，否则 accept 来的新连接无法被分发
void Server::start() {
    threadPool_->start();
    acceptor_->listen();

    if (heartbeatTimeout_ > std::chrono::milliseconds(0)) {
        // 注册定时器：每 heartbeatInterval_ 扫描一次连接表
        // start() 在 main 线程、mainLoop.loop() 之前调用，runInLoop 同线程直接执行，安全
        loop_->runEvery(heartbeatInterval_, [this] { checkHeartbeats(); });
        std::cout << "[Server] heartbeat enabled (timeout="
                  << heartbeatTimeout_.count() << "ms, interval="
                  << heartbeatInterval_.count() << "ms)" << std::endl;
    }

    std::cout << "[Server] started  (1 main reactor + "
              << ioThreadCount_ << " sub-reactor(s))" << std::endl;
}

// ========== 心跳扫描 ==========

// checkHeartbeats — 在 main reactor 线程上由定时器周期性调用
// 遍历连接表快照，空闲超过 heartbeatTimeout_ 的连接 forceClose
// 说明：
//   - getAllConnections() 返回 shared_ptr 快照，扫描期间连接即使并发关闭也不会悬垂
//   - forceClose() 线程安全，内部 runInLoop 调度到连接所属 sub-reactor 线程
//   - 被踢的连接走 forceClose → handleClose → closeCallback，应用层会收到"xx left"广播
void Server::checkHeartbeats() {
    for (auto& conn : getAllConnections()) {
        if (conn->isIdle(heartbeatTimeout_)) {
            std::cout << "[Server] force close idle " << conn->name()
                      << " (fd=" << conn->fd() << ")" << std::endl;
            conn->forceClose();
        }
    }
}

// ========== 新连接（在主 reactor 线程上调用）==========

// onNewConnection — 接受新连接后的处理入口
// 在主 Reactor 线程上由 Acceptor::handleRead 回调调用
//
// 流程：
//   1. 从线程池中选择一个 sub-reactor（round-robin）
//   2. 在 sub-reactor 线程上通过 runInLoop 调度创建 TcpConnection
//      为什么要在 sub-reactor 线程上创建？
//      → TcpConnection 的 Channel::enableReading() 会调用 epoll_ctl
//        把 connfd 加入 EventLoop 的 epoll 实例。这个 epoll 实例
//        属于 sub-reactor，所以必须在 sub-reactor 线程上操作。
//   3. 在 sub-reactor 线程上注册回调、存到 connections_、调用 connectionCallback_
void Server::onNewConnection(int connfd, const sockaddr_in& addr) {
    EventLoop* ioLoop = threadPool_->getNextLoop();

    ioLoop->runInLoop([this, ioLoop, connfd, addr]() {
        auto conn = std::make_shared<TcpConnection>(ioLoop, connfd, addr);

        std::string connName = conn->name();

        conn->setMessageCallback(
            [this](TcpConnection* c, Buffer* b) { onMessage(c, b); });
        conn->setCloseCallback(
            [this](TcpConnection* c) { onCloseConnection(c); });

        int fd = conn->fd();
        {
            std::lock_guard<std::mutex> lock(connectionsMutex_);
            connections_[fd] = conn;
        }

        std::cout << "[Server] new " << connName
                  << " on reactor thread (fd=" << fd << ")" << std::endl;

        if (connectionCallback_) {
            connectionCallback_(conn.get());
        }
    });
}

// ========== 消息处理（在 sub-reactor 线程上调用）==========

// 在 sub-reactor 线程上直接转发到用户注册的 messageCallback_
void Server::onMessage(TcpConnection* conn, Buffer* buf) {
    if (messageCallback_) {
        messageCallback_(conn, buf);
    }
}

// ========== 连接关闭（在 sub-reactor 线程上调用）==========

// 在 sub-reactor 线程上移除连接
// connections_ 的 erase 与主 reactor 的 insert 可能并发，用 mutex 保护
//
// 顺序：先通知应用层（closeCallback_），再 erase。
// 这样应用层回调里仍能通过 conn 访问到该连接（connections_ 还持有 shared_ptr），
// 适合做"xx 离开了聊天室"之类的清理或广播，然后再真正回收。
void Server::onCloseConnection(TcpConnection* conn) {
    int fd = conn->fd();
    std::cout << "[Server] " << conn->name() << " closed (fd=" << fd << ")"
              << std::endl;

    if (closeCallback_) {
        closeCallback_(conn);
    }

    std::lock_guard<std::mutex> lock(connectionsMutex_);
    connections_.erase(fd);
}

// 获取所有活跃连接
// 返回 shared_ptr 而不是裸指针：
//   裸指针无法阻止并发关闭导致的悬垂；shared_ptr 在调用方持有期间保证对象存活。
// 调用方拿到后建议立即使用，不要长期持有（连接关闭后迟早会析构）。
std::vector<std::shared_ptr<TcpConnection>> Server::getAllConnections() {
    std::lock_guard<std::mutex> lock(connectionsMutex_);
    std::vector<std::shared_ptr<TcpConnection>> v;
    v.reserve(connections_.size());
    for (auto& [fd, conn] : connections_) {
        (void)fd;
        v.push_back(conn);
    }
    return v;
}
