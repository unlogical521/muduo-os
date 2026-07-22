//
// Acceptor 实现
//
#include "Acceptor.h"
#include "Channel.h"
#include "EventLoop.h"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

Acceptor::Acceptor(EventLoop* loop, int port)
    : loop_(loop), listening_(false) {
    // 1. 创建监听 socket
    accept_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (accept_fd_ < 0) {
        std::cerr << "Acceptor: socket failed" << std::endl;
        abort();
    }

    // 2. 端口复用
    int opt = 1;
    ::setsockopt(accept_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 绑定地址
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (::bind(accept_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Acceptor: bind failed" << std::endl;
        ::close(accept_fd_);
        abort();
    }

    // 4. 创建 Channel，但不立即监听（listen() 时才加入 EventLoop）
    accept_channel_ = std::make_unique<Channel>(loop_, accept_fd_);
    accept_channel_->setReadCallback([this] { handleRead(); });
}

Acceptor::~Acceptor() {
    accept_channel_->disableAll();
    ::close(accept_fd_);
}

void Acceptor::listen() {
    // 开始监听
    if (::listen(accept_fd_, 128) < 0) {
        std::cerr << "Acceptor: listen failed" << std::endl;
        return;
    }
    listening_ = true;

    // 将监听 fd 加入 EventLoop，关注可读事件
    accept_channel_->enableReading();
}

void Acceptor::handleRead() {
    // 接受所有新连接（非阻塞 accept 循环，避免 LT 模式下重复触发）
    while (true) {
        sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int connfd = ::accept4(accept_fd_, (sockaddr*)&client_addr,
                               &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (connfd < 0) {
            // EAGAIN / EWOULDBLOCK：没有更多新连接
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            // 其他错误
            std::cerr << "Acceptor: accept failed errno=" << errno << std::endl;
            break;
        }

        // 通知上层处理新连接
        if (newConnectionCallback_) {
            newConnectionCallback_(connfd, client_addr);
        } else {
            // 没有回调就关闭
            ::close(connfd);
        }
    }
}
