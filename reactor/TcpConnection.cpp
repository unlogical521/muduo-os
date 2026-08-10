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

// 构造函数
// 在 sub-reactor 线程上执行（由 Server::onNewConnection 通过 runInLoop 调度）
// 创建 Channel、注册回调，默认关注可读事件
TcpConnection::TcpConnection(EventLoop* loop, int connfd, const sockaddr_in& addr)
    : loop_(loop),
      connfd_(connfd),
      last_active_(std::chrono::steady_clock::now()) {
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
    // 析构时由 Channel 析构函数自动处理 epoll 摘除（若 events_ 不为 0）
    // close(fd) 会自动清理内核中该 fd 关联的 epoll 注册，
    // 但为防止 Channel 析构时的 disableAll 重复调用 epoll_ctl DEL，已在 handleClose 中调用
    ::close(connfd_);
}

// ========== 线程安全 send ==========

// send(const string&) 委托给 send(const char*, size_t)
void TcpConnection::send(const std::string& message) {
    send(message.data(), message.size());
}

// send(const char*, size_t) — 线程安全的发送接口
// 跨线程调用时的处理流程：
//   1. 拷贝 data 到 std::string msg（确保数据在 lambda 捕获时持活）
//   2. 通过 runInLoop 调度到 loop_ 线程
//   3. 在 loop_ 线程上调用 sendInLoop
// 注意：
//   - lambda 按值捕获 msg 和 shared_from_this()
//   - shared_from_this() 确保 TcpConnection 在执行 lambda 时仍然存活
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

// sendInLoop — 在 loop_ 线程上执行的实际发送逻辑
// 写策略（"直接写优先，缓冲区兜底"）：
//   1. output_buffer_ 为空 → 说明没有积压数据，尝试直接 write(fd, data)
//     - 一次写完（nwritten == len）：完美路径，无需触发 EPOLLOUT
//     - 部分写完（nwritten < len）：剩余数据入 output_buffer_，关注 EPOLLOUT
//     - 写阻塞（EAGAIN）：当作 nwritten = 0，全部入 output_buffer_
//   2. output_buffer_ 非空 → 说明还有积压数据，先入 output_buffer_，等待 handleWrite
//
// 为什么先试 write 而不是直接入 buffer？
//   - 如果每次 send 都入 buffer + enableWriting，会至少多一次 epoll_wait 才写出去
//   - 对于小数据包（如大多数业务消息），直接 write 一次就能写完，延迟更低
void TcpConnection::sendInLoop(const char* data, size_t len) {
    ssize_t nwritten = 0;
    if (output_buffer_->readableBytes() == 0) {
        // output buffer 为空 → 尝试直接 write（内核写缓冲区大概率有空闲）
        nwritten = ::write(connfd_, data, len);
        if (nwritten < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核写缓冲区满，本次无法写入任何数据
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
        // 剩余数据写入 output buffer，并关注可写事件
        // 当内核缓冲区可写时，epoll 会通知 EPOLLOUT，触发 handleWrite
        output_buffer_->append(data + nwritten, remaining);
        channel_->enableWriting();
    } else {
        // 全部写完，触发写完成回调（如果注册了）
        if (writeCompleteCallback_) {
            writeCompleteCallback_(this);
        }
    }
}

// ========== 关闭 ==========

// shutdown — 半关闭连接（关闭写端）
// 线程安全：同 send()，通过 runInLoop 调度到 loop_ 线程
void TcpConnection::shutdown() {
    if (loop_->isInLoopThread()) {
        shutdownInLoop();
    } else {
        loop_->runInLoop([self = shared_from_this()]() {
            self->shutdownInLoop();
        });
    }
}

// shutdownInLoop — 在 loop_ 线程执行实际的 shutdown
// SHUT_WR：关闭写方向（不能再 send），但仍可 recv
// 这是优雅关闭的第一步（等读完所有数据后再 close）
void TcpConnection::shutdownInLoop() {
    if (::shutdown(connfd_, SHUT_WR) < 0) {
        std::cerr << "TcpConnection::shutdownInLoop error name=" << name_ << std::endl;
    }
}

// ========== 强制关闭（踢人） ==========

// forceClose — 线程安全：把真正的关闭调度到所属 loop_ 线程
// 与 send() 的跨线程模式一致：捕获 shared_from_this() 保证对象在执行时仍存活
void TcpConnection::forceClose() {
    if (loop_->isInLoopThread()) {
        forceCloseInLoop();
    } else {
        loop_->runInLoop([self = shared_from_this()]() {
            self->forceCloseInLoop();
        });
    }
}

// forceCloseInLoop — 在 loop_ 线程执行强制关闭（含延迟关闭状态机）
//
// 为什么要"延迟关闭"？
//   forceClose 可能在用户回调栈内被调用（如消息回调里踢人）。
//   此时若直接 handleClose → closeCallback → Server::onCloseConnection → erase
//   → shared_ptr 引用归零 → 本对象（this）被析构，但调用者仍处于本对象的成员函数栈上，
//   继续访问成员就是未定义行为（悬垂）。
//
// 解法（muduo 标准做法）：第一次调用只标记 kDisconnecting，并把自己重新排入
// pendingFunctors（在 doPendingFunctors 执行时，当前事件回调栈已经退出）。
// 第二次进入时栈已安全，才真正 handleClose。
//
// 流程：
//   kConnected      → 置 kDisconnecting，重新入队（本次不关）
//   kDisconnecting  → 第二次到达：事件栈已退，handleClose 安全
void TcpConnection::forceCloseInLoop() {
    if (state_ == State::kConnected) {
        state_ = State::kDisconnecting;
        loop_->queueInLoop([self = shared_from_this()]() {
            self->forceCloseInLoop();
        });
    } else if (state_ == State::kDisconnecting) {
        handleClose();
    }
}

// ========== IO 事件处理（均在所属 loop_ 线程上被 Channel 回调调用）==========

// handleRead — fd 可读时被调用
// 从 fd 读取数据存入 input_buffer_，然后通知上层
// 返回值三种情况：
//   > 0 : 正常读取了 n 字节 → 调用 messageCallback_
//   = 0 : 对端关闭（EOF） → 调用 handleClose
//   < 0 : 读错误 → 调用 handleError
void TcpConnection::handleRead() {
    ssize_t n = input_buffer_->readFd(connfd_);
    if (n > 0) {
        // 收到数据 → 记录活跃时间，心跳扫描据此判定连接是否空闲
        last_active_ = std::chrono::steady_clock::now();
        if (messageCallback_) {
            messageCallback_(this, input_buffer_.get());
        }
    } else if (n == 0) {
        handleClose();
    } else {
        handleError();
    }
}

// handleWrite — fd 可写时被调用（由 enableWriting 注册的 EPOLLOUT 触发）
// 将 output_buffer_ 中积压的数据写入 fd
// 如果全部写完了，取消 EPOLLOUT 关注
void TcpConnection::handleWrite() {
    ssize_t n = ::write(connfd_,
                        output_buffer_->peek(),
                        output_buffer_->readableBytes());
    if (n > 0) {
        output_buffer_->retrieve(n);
        if (output_buffer_->readableBytes() == 0) {
            // 全部写完，不再关注可写事件
            // 这样 epoll 就不会因 EPOLLOUT 反复返回（水平触发特性）
            channel_->disableWriting();
            if (writeCompleteCallback_) {
                writeCompleteCallback_(this);
            }
        }
    } else {
        std::cerr << "TcpConnection::handleWrite error name=" << name_ << std::endl;
    }
}

// handleClose — 连接关闭时的处理
// 1. 从 epoll 中移除对该 fd 的监听（disableAll）
// 2. 通知上层连接已关闭（closeCallback_ → Server::onCloseConnection）
// 注意：不在这里 close(fd)，因为 TcpConnection 析构时会 close
void TcpConnection::handleClose() {
    state_ = State::kDisconnected;
    channel_->disableAll();
    if (closeCallback_) {
        closeCallback_(this);
    }
}

// handleError — 处理 fd 上的错误
// 仅打印日志，具体重连或恢复策略由上层业务决定
void TcpConnection::handleError() {
    std::cerr << "TcpConnection[" << name_ << "] error on fd=" << connfd_ << std::endl;
}
