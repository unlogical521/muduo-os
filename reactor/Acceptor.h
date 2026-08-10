//
// Acceptor — 封装监听 fd 的 accept 操作
//
// 内部创建一个监听 socket，bind 到指定地址和端口，
// 然后注册一个 Channel 到 EventLoop。
// 当监听 fd 可读时，accept() 新连接并通过回调通知上层。
//
// 线程安全：
//   - Acceptor 在 main reactor 创建，只属于 main reactor 线程
//   - accept() 都在 main reactor 线程上执行
//   - newConnectionCallback_ 负责将新连接分发给 sub-reactor
//
#pragma once

#include <functional>
#include <memory>
#include <netinet/in.h>

class EventLoop;
class Channel;

class Acceptor {
public:
    using NewConnectionCallback = std::function<void(int connfd, const sockaddr_in& addr)>;

    Acceptor(EventLoop* loop, int port);
    ~Acceptor();

    // 禁止拷贝（持有 unique_ptr Channel）
    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    // 注册新连接回调（由 Server 在构造函数中设置）
    void setNewConnectionCallback(NewConnectionCallback cb) {
        newConnectionCallback_ = std::move(cb);
    }

    bool listening() const { return listening_; }

    // 开始监听（调用 listen + enableReading 注册到 EventLoop）
    void listen();

private:
    // 监听 fd 可读时被调用（accept 所有新连接）
    void handleRead();

    EventLoop* loop_;
    int accept_fd_;                          // 监听 socket
    std::unique_ptr<Channel> accept_channel_;
    NewConnectionCallback newConnectionCallback_;
    bool listening_;
};
