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

void Server::start() {
    threadPool_->start();
    acceptor_->listen();

    std::cout << "[Server] started  (1 main reactor + "
              << ioThreadCount_ << " sub-reactor(s))" << std::endl;
}

// ========== 新连接（在主 reactor 线程上调用）==========

void Server::onNewConnection(int connfd, const sockaddr_in& addr) {
    // 选择一个 sub-reactor
    EventLoop* ioLoop = threadPool_->getNextLoop();

    // 在 sub-reactor 线程上创建 TcpConnection
    ioLoop->runInLoop([this, ioLoop, connfd, addr]() {
        auto conn = std::make_shared<TcpConnection>(ioLoop, connfd, addr);

        // 连接名用于日志
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

void Server::onMessage(TcpConnection* conn, Buffer* buf) {
    if (messageCallback_) {
        messageCallback_(conn, buf);
    }
}

// ========== 连接关闭（在 sub-reactor 线程上调用）==========

void Server::onCloseConnection(TcpConnection* conn) {
    int fd = conn->fd();
    std::cout << "[Server] " << conn->name() << " closed (fd=" << fd << ")"
              << std::endl;

    std::lock_guard<std::mutex> lock(connectionsMutex_);
    connections_.erase(fd);
}
