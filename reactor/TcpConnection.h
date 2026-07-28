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
//   - send() 可在任意线程调用，跨线程时通过 runInLoop 调度
//   - 所有 IO 操作（handleRead/handleWrite）只在其所属 EventLoop 线程上执行
//
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <netinet/in.h>

class EventLoop;
class Channel;
class Buffer;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using MessageCallback = std::function<void(TcpConnection*, Buffer*)>;
    using CloseCallback   = std::function<void(TcpConnection*)>;
    using WriteCompleteCallback = std::function<void(TcpConnection*)>;

    TcpConnection(EventLoop* loop, int connfd, const sockaddr_in& addr);
    ~TcpConnection();

    // 禁止拷贝
    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    int fd() const { return connfd_; }
    const std::string& name() const { return name_; }
    EventLoop* getLoop() const { return loop_; }

    // 发送数据（线程安全）
    void send(const std::string& message);
    void send(const char* data, size_t len);

    // 关闭连接写端
    void shutdown();

    // 设置回调
    void setMessageCallback(MessageCallback cb)       { messageCallback_       = std::move(cb); }
    void setCloseCallback(CloseCallback cb)           { closeCallback_         = std::move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb) { writeCompleteCallback_ = std::move(cb); }

private:
    // 以下 3 个函数只在 loop_ 线程上执行
    void sendInLoop(const char* data, size_t len);
    void shutdownInLoop();

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    EventLoop* loop_;
    int connfd_;
    std::string name_;
    std::unique_ptr<Channel> channel_;

    std::unique_ptr<Buffer> input_buffer_;   // 从 fd 收到的数据
    std::unique_ptr<Buffer> output_buffer_;  // 待发送到 fd 的数据

    MessageCallback       messageCallback_;
    CloseCallback         closeCallback_;
    WriteCompleteCallback writeCompleteCallback_;
};
