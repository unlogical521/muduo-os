//
// TcpConnection 实现
//
#include "TcpConnection.h"
#include "Channel.h"
#include "Buffer.h"
#include "EventLoop.h"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

namespace {
    int conn_count = 0;
}

TcpConnection::TcpConnection(EventLoop* loop, int connfd, const sockaddr_in& addr)
    : loop_(loop), connfd_(connfd) {
    name_ = "conn-" + std::to_string(++conn_count);

    input_buffer_  = std::make_unique<Buffer>();
    output_buffer_ = std::make_unique<Buffer>();

    channel_ = std::make_unique<Channel>(loop, connfd);
    channel_->setReadCallback([this] { handleRead(); });
    channel_->setWriteCallback([this] { handleWrite(); });
    channel_->setCloseCallback([this] { handleClose(); });
    channel_->setErrorCallback([this] { handleError(); });

    // 默认关注可读事件（在构造函数中调用，必须在正确的 loop 线程）
    channel_->enableReading();
}

TcpConnection::~TcpConnection() {
    // handleClose() 已通过 disableAll() 从 epoll 移除；
    // 副作用的 fd 由 OS 在 close() 时自动清理 epoll 注册
    ::close(connfd_);
}

// ========== 线程安全 send ==========

void TcpConnection::send(const std::string& message) {
    send(message.data(), message.size());
}

void TcpConnection::send(const char* data, size_t len) {
    if (loop_->isInLoopThread()) {
        sendInLoop(data, len);
    } else {
        // 跨线程调用 → 拷贝数据，通过 runInLoop 调度到 loop 线程
        std::string msg(data, len);
        loop_->runInLoop([self = shared_from_this(), msg]() {
            self->sendInLoop(msg.data(), msg.size());
        });
    }
}

void TcpConnection::sendInLoop(const char* data, size_t len) {
    ssize_t nwritten = 0;
    if (output_buffer_->readableBytes() == 0) {
        // output buffer 为空 → 尝试直接 write
        nwritten = ::write(connfd_, data, len);
        if (nwritten < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nwritten = 0;
            } else {
                std::cerr << "TcpConnection::sendInLoop write error"
                          << " name=" << name_ << std::endl;
                return;
            }
        }
    }

    size_t remaining = len - nwritten;
    if (remaining > 0) {
        output_buffer_->append(data + nwritten, remaining);
        channel_->enableWriting();
    } else {
        if (writeCompleteCallback_) {
            writeCompleteCallback_(this);
        }
    }
}

// ========== 关闭 ==========

void TcpConnection::shutdown() {
    if (loop_->isInLoopThread()) {
        shutdownInLoop();
    } else {
        loop_->runInLoop([self = shared_from_this()]() {
            self->shutdownInLoop();
        });
    }
}

void TcpConnection::shutdownInLoop() {
    if (::shutdown(connfd_, SHUT_WR) < 0) {
        std::cerr << "TcpConnection::shutdownInLoop error name=" << name_ << std::endl;
    }
}

// ========== IO 事件处理（均在 loop_ 线程）==========

void TcpConnection::handleRead() {
    ssize_t n = input_buffer_->readFd(connfd_);
    if (n > 0) {
        if (messageCallback_) {
            messageCallback_(this, input_buffer_.get());
        }
    } else if (n == 0) {
        handleClose();
    } else {
        handleError();
    }
}

void TcpConnection::handleWrite() {
    ssize_t n = ::write(connfd_,
                        output_buffer_->peek(),
                        output_buffer_->readableBytes());
    if (n > 0) {
        output_buffer_->retrieve(n);
        if (output_buffer_->readableBytes() == 0) {
            channel_->disableWriting();
            if (writeCompleteCallback_) {
                writeCompleteCallback_(this);
            }
        }
    } else {
        std::cerr << "TcpConnection::handleWrite error name=" << name_ << std::endl;
    }
}

void TcpConnection::handleClose() {
    channel_->disableAll();
    if (closeCallback_) {
        closeCallback_(this);
    }
}

void TcpConnection::handleError() {
    std::cerr << "TcpConnection[" << name_ << "] error on fd=" << connfd_ << std::endl;
}
