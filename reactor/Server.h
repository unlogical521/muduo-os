//
// Server — 应用层服务器（多 Reactor 版本）
//
// ┌──────────────────────────────────────────┐
// │  Main Reactor (baseLoop_)                 │
// │  ┌──────────┐                             │
// │  │ Acceptor │ → accept → getNextLoop()    │
// │  └──────────┘        ↓                    │
// └────────────────────┼─────────────────────┘
//                      │ runInLoop
//          ┌───────────┼───────────┐
//          ▼           ▼           ▼
//   SubReactor[0]  SubReactor[1]  SubReactor[2]
//   TcpConn × N    TcpConn × N    TcpConn × N
//
// 关键设计：
//   - Acceptor 在 baseLoop_（主 reactor）上 accept
//   - 新连接通过 ioLoop->runInLoop() 调度到 sub-reactor 线程创建
//   - 连接生命周期完全在所属 sub-reactor 上管理
//   - connections_ 用 shared_ptr + mutex 跨线程安全访问
//
#pragma once

#include <functional>
#include <memory>
#include <netinet/in.h>
#include <map>
#include <string>
#include <mutex>

class EventLoop;
class Acceptor;
class TcpConnection;
class Buffer;
class EventLoopThreadPool;

class Server {
public:
    using MessageCallback = std::function<void(TcpConnection*, Buffer*)>;
    using ConnectionCallback = std::function<void(TcpConnection*)>;

    Server(EventLoop* loop, int port);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // 启动服务器
    void start();

    // 设置 IO 线程数（sub-reactor 数量），默认 0 = 单线程（主 reactor 处理全部）
    void setThreadNum(int num);

    // 设置回调
    void setMessageCallback(MessageCallback cb)        { messageCallback_    = std::move(cb); }
    void setConnectionCallback(ConnectionCallback cb)  { connectionCallback_ = std::move(cb); }

private:
    // Acceptor 回调 —— 新连接到来（在主 reactor 线程执行）
    void onNewConnection(int connfd, const sockaddr_in& addr);
    // TcpConnection 消息回调（在 sub-reactor 线程执行）
    void onMessage(TcpConnection* conn, Buffer* buf);
    // TcpConnection 关闭回调（在 sub-reactor 线程执行）
    void onCloseConnection(TcpConnection* conn);

    EventLoop* loop_;                             // 主 reactor
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> threadPool_;
    int ioThreadCount_{0};                        // sub-reactor 数量

    std::map<int, std::shared_ptr<TcpConnection>> connections_;
    std::mutex connectionsMutex_;

    MessageCallback    messageCallback_;
    ConnectionCallback connectionCallback_;
};
