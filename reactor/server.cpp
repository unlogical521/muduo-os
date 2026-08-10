//
// 基于多 Reactor 多线程的 epoll 聊天室（演示 Server 的断连回调 + 连接表）
//
// 用法：
//   ./re_server                  ← 启动（2 个 sub-reactor 线程）
//   ./re_server 9090             ← 指定端口
//   ./re_server 9090 4           ← 指定端口 + 4 个 sub-reactor 线程
//   ./re_server 8080 0           ← 单线程模式（全部在主 reactor）
//
// 本 demo 展示 Server 的三个新增能力：
//   1. setCloseCallback     → 连接断开时通知应用层（广播 "xx left"）
//   2. getAllConnections()  → 拿到全连接表做广播（聊天室的核心）
//   3. setHeartbeat        → 心跳超时检测，空闲连接自动踢出（拔网线/进程崩溃的半开连接回收）
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
#include <iostream>
#include <memory>
#include <cstring>
#include <unistd.h>
#include <thread>
#include <chrono>

#include "EventLoop.h"
#include "Server.h"
#include "TcpConnection.h"
#include "Buffer.h"

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

    std::cout << "=== Multi-Reactor Chat Server ===" << std::endl;
    std::cout << "Port:       " << port << std::endl;
    std::cout << "IO threads: " << ioThreads << " (sub-reactors)" << std::endl;
    std::cout << "==================================" << std::endl;

    // 创建主 Reactor 的 EventLoop（在 main 线程上运行）
    EventLoop mainLoop;

    // 创建服务器（Acceptor 注册在 mainLoop）
    Server server(&mainLoop, port);
    server.setThreadNum(ioThreads);

    // 开启心跳：连接空闲 5 秒未收到数据 → 强制断开（并广播 "xx left"）
    // 每 1 秒扫描一次（便于演示，实际可放宽）
    server.setHeartbeat(std::chrono::seconds(5), std::chrono::seconds(1));

    // 连接回调 —— 新连接上线（在 sub-reactor 线程执行）
    // 广播"xx joined"，并给新人单独发欢迎语（含自己的名字）
    server.setConnectionCallback([&server](TcpConnection* conn) {
        std::cout << "[connection] " << conn->name()
                  << " handled by thread "
                  << std::this_thread::get_id()
                  << std::endl;

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
        std::cout << "[message] from " << conn->name()
                  << " on thread " << std::this_thread::get_id()
                  << ": " << log << std::endl;

        broadcast(server, nullptr, "[chat] " + conn->name() + " " + msg);
    });

    // 断连回调 —— 新增能力（在 sub-reactor 线程执行）
    // 连接还没从 connections_ 移除（erase 在回调之后），但仍广播给除它以外的所有人
    server.setCloseCallback([&server](TcpConnection* conn) {
        std::cout << "[close] " << conn->name() << " disconnected" << std::endl;
        broadcast(server, conn, "[chat] " + conn->name() + " left\r\n");
    });

    // 启动服务器（start 内部会启动 sub-reactor 线程池）
    server.start();

    // 主 Reactor 进入事件循环（mainLoop 在 main 线程运行）
    // 主 Reactor 负责任务：监听 fd 的 accept + 将新连接分发给 sub-reactor
    mainLoop.loop();

    return 0;
}
