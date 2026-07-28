//
// Acceptor — 封装监听 fd 的 accept 操作
//
// 内部创建一个监听 socket，bind 到指定地址和端口，
// 然后注册一个 Channel 到 EventLoop。
// 当监听 fd 可读时，accept() 新连接并通过回调通知上层。
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

    // 禁止拷贝
    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    void setNewConnectionCallback(NewConnectionCallback cb) {
        newConnectionCallback_ = std::move(cb);
    }

    bool listening() const { return listening_; }
    void listen();

private:
    void handleRead();

    EventLoop* loop_;
    int accept_fd_;                          // 监听 socket
    std::unique_ptr<Channel> accept_channel_;
    NewConnectionCallback newConnectionCallback_;
    bool listening_;
};
