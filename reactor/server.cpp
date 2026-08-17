//
// 基于多 Reactor 多线程的 epoll 聊天室（演示异步日志 + 心跳 + 断连回调 + 连接表）
//
// 用法：
//   ./re_server                  ← 启动（2 个 sub-reactor 线程）
//   ./re_server 9090             ← 指定端口
//   ./re_server 9090 4           ← 指定端口 + 4 个 sub-reactor 线程
//   ./re_server 8080 0           ← 单线程模式（全部在主 reactor）
//
// 本 demo 展示的库能力：
//   1. 异步日志   → 全部日志走 AsyncLogging（双缓冲 + 后台线程），不阻塞 IO 线程
//   2. 心跳       → 空闲超时连接自动强制关闭（回收半开连接）
//   3. setCloseCallback   → 连接断开时通知应用层（广播 "xx left"）
//   4. getAllConnections → 拿到全连接表做广播（聊天室核心）
//
// 架构（多 Reactor）：
//
//   Main Reactor (main thread)
//   ┌──────────────────────────┐
//   │ accept → 分发到 sub-reactor│
//   └──────────┬───────────────┘
//              │ runInLoop()
//     ┌────────┼────────┐
//     ▼        ▼        ▼
//   SubR[0]  SubR[1]  SubR[2]  (EventLoopThread 线程池)
//   ┌────┐   ┌────┐   ┌────┐
//   │IO  │   │IO  │   │IO  │
//   │重  │   │重  │   │重  │
//   │任  │   │任  │   │任  │
//   └────┘   └────┘   └────┘
//
#include <memory>
#include <cstring>
#include <unistd.h>
#include <chrono>
#include <csignal>

#include "EventLoop.h"
#include "Server.h"
#include "TcpConnection.h"
#include "Buffer.h"
#include "Logger.h"
#include "AsyncLogging.h"

// ========== 异步日志初始化 ==========
// 进程级单例：日志写 server.*.pid.log，按大小滚动，每 3 秒强制落盘
AsyncLogging g_asyncLog("server", 100 * 1024 * 1024, 3);

// 主事件循环指针（供信号处理器触发优雅退出）
EventLoop* g_mainLoop = nullptr;

// 把 Logger 的输出/刷新接到异步日志后端
// 此后所有 LOG_XXX 都走双缓冲 + 后台线程，IO 线程不碰磁盘
static void initLogging() {
    Logger::setOutput([](const char* msg, int len) {
        g_asyncLog.append(msg, len);
    });
    Logger::setFlush([]() {
        g_asyncLog.flush();
    });
    g_asyncLog.start();
    LOG_INFO << "[logging] async logging started (server.*.log)";
}

// Ctrl-C / kill → 优雅退出：quit 事件循环 → main 返回 → 全局对象析构 → 日志落盘
static void signalHandler(int) {
    if (g_mainLoop) {
        g_mainLoop->quit();
    }
}

// 广播一条消息到所有连接（可选排除某个连接）
// from == nullptr  → 发给所有人（包括发消息的人，聊天室"自己也看得到"）
// from != nullptr  → 发给除 from 以外的所有人（如 "xx left" 不需要发给 xx）
//
// 线程安全说明：
//   getAllConnections() 返回 shared_ptr 快照，遍历时连接即使并发关闭也不会悬垂；
//   conn->send() 内部跨线程走 runInLoop + eventfd 唤醒，本身线程安全。
static void broadcast(Server& server, TcpConnection* from, const std::string& msg) {
    for (auto& conn : server.getAllConnections()) {
        if (from && conn.get() == from) continue;
        conn->send(msg);
    }
}

int main(int argc, char* argv[]) {
    int port = 8080;
    int ioThreads = 2;  // 默认 2 个 sub-reactor 线程（不含 main reactor）

    if (argc > 1) port = std::atoi(argv[1]);
    if (argc > 2) ioThreads = std::atoi(argv[2]);

    // 先初始化日志，后续所有输出都进异步日志文件
    initLogging();

    LOG_INFO << "=== Multi-Reactor Chat Server ===";
    LOG_INFO << "Port: " << port << ", IO threads: " << ioThreads << " (sub-reactors)";

    // 创建主 Reactor 的 EventLoop（在 main 线程上运行）
    EventLoop mainLoop;
    g_mainLoop = &mainLoop;

    // 注册信号：Ctrl-C / kill 优雅退出
    ::signal(SIGINT, signalHandler);
    ::signal(SIGTERM, signalHandler);

    // 创建服务器（Acceptor 注册在 mainLoop）
    Server server(&mainLoop, port);
    server.setThreadNum(ioThreads);

    // 开启心跳：连接空闲 5 秒未收到数据 → 强制断开（并广播 "xx left"）
    // 每 1 秒扫描一次（便于演示，实际可放宽）
    server.setHeartbeat(std::chrono::seconds(5), std::chrono::seconds(1));

    // 连接回调 —— 新连接上线（在 sub-reactor 线程执行）
    // 广播"xx joined"，并给新人单独发欢迎语（含自己的名字）
    // 日志格式头自带线程 id，可据此观察连接被分发到哪个 reactor 线程
    server.setConnectionCallback([&server](TcpConnection* conn) {
        LOG_INFO << "[connection] " << conn->name() << " connected";

        broadcast(server, conn, "[chat] " + conn->name() + " joined\r\n");
        conn->send("[chat] welcome, you are " + conn->name() + "\r\n");
    });

    // 消息回调 —— 聊天室转发（在 sub-reactor 线程执行）
    // 所有人（包括自己）都会收到： "[conn-1] hello"
    server.setMessageCallback([&server](TcpConnection* conn, Buffer* buf) {
        std::string msg = buf->retrieveAllAsString();

        // 去掉末尾换行仅用于日志显示
        std::string log = msg;
        while (!log.empty() && (log.back() == '\n' || log.back() == '\r')) {
            log.pop_back();
        }
        LOG_INFO << "[message] from " << conn->name() << ": " << log;

        broadcast(server, nullptr, "[chat] " + conn->name() + " " + msg);
    });

    // 断连回调 —— 新增能力（在 sub-reactor 线程执行）
    // 连接还没从 connections_ 移除（erase 在回调之后），但仍广播给除它以外的所有人
    server.setCloseCallback([&server](TcpConnection* conn) {
        LOG_INFO << "[close] " << conn->name() << " disconnected";
        broadcast(server, conn, "[chat] " + conn->name() + " left\r\n");
    });

    // 启动服务器（start 内部会启动 sub-reactor 线程池 + 心跳定时器）
    server.start();

    // 主 Reactor 进入事件循环（mainLoop 在 main 线程运行）
    // 主 Reactor 负责任务：监听 fd 的 accept + 将新连接分发给 sub-reactor
    mainLoop.loop();

    // 优雅退出路径：Ctrl-C 触发后走到这里
    LOG_INFO << "[server] event loop quit, shutting down...";
    return 0;
}
