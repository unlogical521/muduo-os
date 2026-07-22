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

} // anonymous namespace

TcpConnection::TcpConnection(EventLoop* loop, int connfd, sockaddr_in& addr)
    : loop_(loop), connfd_(connfd) {
    name_ = "conn-" + std::to_string(++conn_count);

    input_buffer_  = std::make_unique<Buffer>();
    output_buffer_ = std::make_unique<Buffer>();

    channel_ = std::make_unique<Channel>(loop, connfd);
    channel_->setReadCallback([this] { handleRead(); });
    channel_->setWriteCallback([this] { handleWrite(); });
    channel_->setCloseCallback([this] { handleClose(); });
    channel_->setErrorCallback([this] { handleError(); });

    // 默认关注可读事件
    channel_->enableReading();
}

TcpConnection::~TcpConnection() {
    channel_->disableAll();
    ::close(connfd_);
}

void TcpConnection::send(const std::string& message) {
    send(message.data(), message.size());
}

void TcpConnection::send(const char* data, size_t len) {
    // 如果 output_buffer 为空，先尝试直接 write
    // 如果一次写不完，剩下的放 buffer，关注可写事件
    ssize_t nwritten = 0;
    if (output_buffer_->readableBytes() == 0) {
        nwritten = ::write(connfd_, data, len);
        if (nwritten < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nwritten = 0;
            } else {
                std::cerr << "TcpConnection::send write error" << std::endl;
                return;
            }
        }
    }

    size_t remaining = len - nwritten;
    if (remaining > 0) {
        // 剩余数据写入 output buffer 并关注可写事件
        output_buffer_->append(data + nwritten, remaining);
        channel_->enableWriting();
    } else {
        // 全部写完，如果有 WriteCompleteCallback 则通知
        if (writeCompleteCallback_) {
            writeCompleteCallback_(this);
        }
    }
}

void TcpConnection::shutdown() {
    // 先关闭写端
    if (::shutdown(connfd_, SHUT_WR) < 0) {
        std::cerr << "TcpConnection::shutdown error" << std::endl;
    }
}

void TcpConnection::handleRead() {
    ssize_t n = input_buffer_->readFd(connfd_);
    if (n > 0) {
        // 有数据可读，触发 messageCallback
        if (messageCallback_) {
            messageCallback_(this, input_buffer_.get());
        }
    } else if (n == 0) {
        // 对方关闭
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
            // 全部写完，不再关注可写事件
            channel_->disableWriting();
            if (writeCompleteCallback_) {
                writeCompleteCallback_(this);
            }
        }
    } else {
        std::cerr << "TcpConnection::handleWrite error" << std::endl;
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
