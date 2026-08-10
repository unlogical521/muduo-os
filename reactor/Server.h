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
//   - 连接创建后的所有 IO 完全在所属 sub-reactor 上管理，无锁
//   - connections_ 用 shared_ptr + mutex 跨线程安全访问
//
// 线程安全：
//   onNewConnection   → 主 reactor 线程调用
//   onMessage         → sub-reactor 线程调用
//   onCloseConnection → sub-reactor 线程调用
//   connections_ 的读写通过 mutex 保护
//
#pragma once

#include <functional>
#include <memory>
#include <netinet/in.h>
#include <map>
#include <string>
#include <mutex>
#include <vector>
#include <chrono>

class EventLoop;
class Acceptor;
class TcpConnection;
class Buffer;
class EventLoopThreadPool;

class Server {
public:
    using MessageCallback = std::function<void(TcpConnection*, Buffer*)>;
    using ConnectionCallback = std::function<void(TcpConnection*)>;
    using CloseCallback = std::function<void(TcpConnection*)>;

    Server(EventLoop* loop, int port);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // 启动服务器
    // 1. 启动 EventLoopThreadPool（创建 sub-reactor 线程池）
    // 2. 开始 listening（Acceptor 注册到主 reactor）
    void start();

    // 设置 IO 线程数（sub-reactor 数量）
    // threadNum = 0 → 单线程模式（主 reactor 处理所有 IO）
    // threadNum = 4 → 1 个主 reactor + 4 个 sub-reactor
    void setThreadNum(int num);

    // 设置回调
    void setMessageCallback(MessageCallback cb)        { messageCallback_    = std::move(cb); }
    void setConnectionCallback(ConnectionCallback cb)  { connectionCallback_ = std::move(cb); }
    // 断连回调：在连接从 connections_ 移除前触发（仍持有该连接，可做清理/广播）
    void setCloseCallback(CloseCallback cb)            { closeCallback_      = std::move(cb); }

    // 获取所有活跃连接（返回 shared_ptr 保证连接即使并发关闭也存活）
    std::vector<std::shared_ptr<TcpConnection>> getAllConnections();

    // 开启心跳超时检测（默认关闭；timeout=0 即关闭）
    // timeout：连接空闲超过该时长（未收到数据）则被强制关闭
    // checkInterval：扫描间隔（默认 2s）。超时精度约为 checkInterval + timeout
    // 实现：在 main reactor 上 runEvery 一个定时器，周期性扫描连接表，
    //       对超时连接调用 forceClose()（跨线程调度到所属 sub-reactor）
    void setHeartbeat(std::chrono::milliseconds timeout,
                      std::chrono::milliseconds checkInterval = std::chrono::seconds(2));

private:
    // Acceptor 回调 —— 新连接到来（在主 reactor 线程执行）
    // round-robin 选择一个 sub-reactor，通过 runInLoop 调度
    void onNewConnection(int connfd, const sockaddr_in& addr);
    // TcpConnection 消息回调（在 sub-reactor 线程执行）
    void onMessage(TcpConnection* conn, Buffer* buf);
    // TcpConnection 关闭回调（在 sub-reactor 线程执行）
    void onCloseConnection(TcpConnection* conn);

    // 心跳扫描（在 main reactor 线程执行，由 runEvery 定时器触发）
    // 遍历连接表，超时未活跃的连接 forceClose
    void checkHeartbeats();

    EventLoop* loop_;                             // 主 reactor（main 线程）
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> threadPool_;
    int ioThreadCount_{0};                        // sub-reactor 数量

    std::chrono::milliseconds heartbeatTimeout_{0};      // 心跳超时（0 = 不启用）
    std::chrono::milliseconds heartbeatInterval_{std::chrono::seconds(2)};  // 扫描间隔

    // 所有活跃连接，由 shared_ptr 管理生命周期
    // 主 reactor（insert）和 sub-reactor（erase）可能并发访问，用 mutex 保护
    std::map<int, std::shared_ptr<TcpConnection>> connections_;
    std::mutex connectionsMutex_;

    MessageCallback    messageCallback_;
    ConnectionCallback connectionCallback_;
    CloseCallback      closeCallback_;
};
