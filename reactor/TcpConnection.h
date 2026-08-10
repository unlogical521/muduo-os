//
// TcpConnection — 封装一个 TCP 连接
//
// 每个客户端连接对应一个 TcpConnection 对象。
// 内部持有：
//   - 通信 fd 的 Channel
//   - 输入/输出 Buffer（Buffer.h）
// 当 fd 可读时：从 fd 读到 inputBuffer_，调用 messageCallback_
// 当 fd 可写时：将 outputBuffer_ 写出到 fd
//
// 线程安全：
//   - send() 可在任意线程调用，跨线程时通过 runInLoop 调度到所属 Reactor 线程
//   - 所有 IO 操作（handleRead/handleWrite）只在其所属 EventLoop 线程上执行
//   - 继承 enable_shared_from_this 确保跨线程回调时对象仍然存活
//
// 生命周期：
//   Server::connections_ 持有 shared_ptr，
//   当连接关闭时从 connections_ 移除，若再无引用则自动析构
//
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <chrono>
#include <netinet/in.h>

class EventLoop;
class Channel;
class Buffer;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using MessageCallback = std::function<void(TcpConnection*, Buffer*)>;
    using CloseCallback   = std::function<void(TcpConnection*)>;
    using WriteCompleteCallback = std::function<void(TcpConnection*)>;

    // 连接状态机：
    //   kConnected      → 正常
    //   kDisconnecting  → 已请求关闭，等待当前事件回调栈退栈后真正关闭（延迟关闭）
    //   kDisconnected   → 已关闭
    enum class State { kConnected, kDisconnecting, kDisconnected };

    TcpConnection(EventLoop* loop, int connfd, const sockaddr_in& addr);
    ~TcpConnection();

    // 禁止拷贝
    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    int fd() const { return connfd_; }
    const std::string& name() const { return name_; }
    EventLoop* getLoop() const { return loop_; }

    // 发送数据（线程安全）
    // 内部处理：同线程 → sendInLoop；异线程 → runInLoop([shared_from_this])
    void send(const std::string& message);
    void send(const char* data, size_t len);

    // 关闭连接写端（半关闭 SHUT_WR），仍可读
    // 线程安全性同 send()
    void shutdown();

    // === 心跳 / 主动关闭 ===

    // 连接是否已空闲超过 timeout（距上次收到数据多久）
    // 由 Server 心跳扫描调用，用于判定半开连接（拔网线、进程崩溃等）
    bool isIdle(std::chrono::milliseconds timeout) const {
        return std::chrono::steady_clock::now() - last_active_ > timeout;
    }

    // 强制关闭连接（线程安全，可跨线程调用）
    // 内部走 runInLoop 调度到所属 loop 线程，延迟到事件回调栈退出后再真正关闭，
    // 避免在用户回调（如 messageCallback）内直接销毁对象
    void forceClose();

    // 设置回调
    void setMessageCallback(MessageCallback cb)       { messageCallback_       = std::move(cb); }
    void setCloseCallback(CloseCallback cb)           { closeCallback_         = std::move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb) { writeCompleteCallback_ = std::move(cb); }

private:
    void sendInLoop(const char* data, size_t len);
    void shutdownInLoop();

    // 在 loop_ 线程上执行的强制关闭（含延迟关闭状态机）
    void forceCloseInLoop();

    // IO 事件处理（均在 loop_ 线程上由 Channel::handleEvent 触发调用）
    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    EventLoop* loop_;                            // 所属 Reactor（sub-reactor）
    int connfd_;                                 // 通信 fd
    std::string name_;                           // 连接名（日志用）
    State state_{State::kConnected};             // 连接状态
    std::chrono::steady_clock::time_point last_active_;  // 上次收到数据的时间（心跳判定用）
    std::unique_ptr<Channel> channel_;

    std::unique_ptr<Buffer> input_buffer_;       // 从 fd 收到的数据（等待应用层处理）
    std::unique_ptr<Buffer> output_buffer_;      // 待发送到 fd 的数据（内核写缓冲区满时暂存）

    MessageCallback       messageCallback_;
    CloseCallback         closeCallback_;
    WriteCompleteCallback writeCompleteCallback_;
};
