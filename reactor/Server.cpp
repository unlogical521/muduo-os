//
// Server 实现
//
#include "Server.h"
#include "EventLoop.h"
#include "Acceptor.h"
#include "TcpConnection.h"
#include "Buffer.h"
#include <iostream>

Server::Server(EventLoop* loop, int port)
    : loop_(loop) {
    acceptor_ = std::make_unique<Acceptor>(loop, port);
    acceptor_->setNewConnectionCallback(
        [this](int connfd, sockaddr_in& addr) {
            onNewConnection(connfd, addr);
        });
}

Server::~Server() = default;

void Server::start() {
    acceptor_->listen();
}

void Server::onNewConnection(int connfd, sockaddr_in& addr) {
    auto conn = std::make_unique<TcpConnection>(loop_, connfd, addr);
    conn->setMessageCallback(
        [this](TcpConnection* c, Buffer* b) { onMessage(c, b); });
    conn->setCloseCallback(
        [this](TcpConnection* c) { onCloseConnection(c); });

    int fd = conn->fd();
    connections_[fd] = std::move(conn);

    std::cout << "Server: new connection [" << fd << "]" << std::endl;

    if (connectionCallback_) {
        connectionCallback_(connections_[fd].get());
    }
}

void Server::onMessage(TcpConnection* conn, Buffer* buf) {
    if (messageCallback_) {
        messageCallback_(conn, buf);
    }
}

void Server::onCloseConnection(TcpConnection* conn) {
    int fd = conn->fd();
    std::cout << "Server: connection [" << fd << "] closed" << std::endl;
    connections_.erase(fd);
}
