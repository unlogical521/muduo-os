//
// Server — 应用层服务器
//
// 组装 Acceptor + TcpConnection，对外提供简单的消息回调接口。
// 当有新连接时创建 TcpConnection，当连接关闭时清理。
//
#pragma once

#include <functional>
#include <memory>
#include <netinet/in.h>
#include <map>
#include <string>

class EventLoop;
class Acceptor;
class TcpConnection;
class Buffer;

class Server {
public:
    using MessageCallback = std::function<void(TcpConnection*, Buffer*)>;
    using ConnectionCallback = std::function<void(TcpConnection*)>;

    Server(EventLoop* loop, int port);
    ~Server();

    // 禁止拷贝
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();

    // 设置回调
    void setMessageCallback(MessageCallback cb)    { messageCallback_    = std::move(cb); }
    void setConnectionCallback(ConnectionCallback cb) { connectionCallback_ = std::move(cb); }

private:
    // Acceptor 回调 —— 新连接到来
    void onNewConnection(int connfd, sockaddr_in& addr);
    // TcpConnection 消息回调
    void onMessage(TcpConnection* conn, Buffer* buf);
    // TcpConnection 关闭回调
    void onCloseConnection(TcpConnection* conn);

    EventLoop* loop_;
    std::unique_ptr<Acceptor> acceptor_;

    // 当前所有活跃连接
    std::map<int, std::unique_ptr<TcpConnection>> connections_;

    MessageCallback    messageCallback_;
    ConnectionCallback connectionCallback_;
};
