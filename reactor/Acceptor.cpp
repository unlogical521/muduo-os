//
// Acceptor 实现
//
#include "Acceptor.h"
#include "Channel.h"
#include "EventLoop.h"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

// 构造函数
// 1. 创建非阻塞监听 socket
// 2. 设置 SO_REUSEADDR 避免 TIME_WAIT 下端口被占用
// 3. 绑定地址和端口
// 4. 创建 Channel（但尚未加入 EventLoop）
Acceptor::Acceptor(EventLoop* loop, int port)
    : loop_(loop), listening_(false) {
    // 1. 创建监听 socket
    // SOCK_NONBLOCK | SOCK_CLOEXEC：非阻塞 + exec 时自动关闭 fd
    accept_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (accept_fd_ < 0) {
        std::cerr << "Acceptor: socket failed" << std::endl;
        abort();
    }

    // 2. 端口复用
    // 避免服务器重启时因 TIME_WAIT 状态导致 bind 失败（"Address already in use"）
    int opt = 1;
    ::setsockopt(accept_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 绑定地址
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;  // 监听所有网络接口
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

// 开始监听
// 1. listen(fd, backlog=128) 开始 TCP 监听
// 2. 将 accept_channel_ 加入 EventLoop，关注 EPOLLIN
//    此后 epoll_wait 会检测新连接事件
void Acceptor::listen() {
    if (::listen(accept_fd_, 128) < 0) {
        std::cerr << "Acceptor: listen failed" << std::endl;
        return;
    }
    listening_ = true;

    accept_channel_->enableReading();
}

// handleRead — 监听 fd 可读时被调用，接受所有新连接
//
// 使用 while 循环处理所有当前已到达的连接（非阻塞 accept）
// 为什么不用单次 accept？
//   水平触发模式下（默认 LT），只要监听 fd 还有新连接未 accept，
//   epoll_wait 会持续返回 EPOLLIN。while 循环一次性 accpet 全部，
//   避免每次只 accept 一个导致"忙等-阻塞-再忙等"的乒乓效应。
//
// 非阻塞 accept：
//   用 accept4 传入 SOCK_NONBLOCK，使新连接 fd 天生非阻塞；
//   同时 SOCK_CLOEXEC 保证 fork+exec 子进程不会继承该 fd。
//
// accept4 相对于 accept 的优势：
//   一次系统调用可同时完成 accept + 设置 SOCK_NONBLOCK | SOCK_CLOEXEC，
//   避免了额外 fcntl 调用的开销。
void Acceptor::handleRead() {
    while (true) {
        sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int connfd = ::accept4(accept_fd_, (sockaddr*)&client_addr,
                               &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (connfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 没有更多新连接，退出循环
                break;
            }
            // 其他错误（如 ENFILE 系统 fd 耗尽）
            std::cerr << "Acceptor: accept failed errno=" << errno << std::endl;
            break;
        }

        // 新连接已建立，通过回调通知上层
        // Server::onNewConnection 中会做 round-robin 分发到 sub-reactor
        if (newConnectionCallback_) {
            newConnectionCallback_(connfd, client_addr);
        } else {
            // 没有回调意味着没有上层处理，直接关闭
            ::close(connfd);
        }
    }
}
