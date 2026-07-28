//
// 基于多 Reactor 多线程的 epoll 服务器（主入口）
//
// 用法：
//   ./re_server            ← 启动（单 reactor，主线程处理所有）
//   ./re_server 8080       ← 指定端口
//   ./re_server 8080 4     ← 指定端口 + 4 个 sub-reactor 线程
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
// 优点：
//   - 每个 sub-reactor 只处理自己负责的 fd，无锁 IO
//   - 多核并行，水平扩展
//   - 线程池避免频繁创建/销毁线程
//
#include <iostream>
#include <memory>
#include <cstring>
#include <unistd.h>
#include <thread>

#include "EventLoop.h"
#include "Server.h"
#include "TcpConnection.h"
#include "Buffer.h"

int main(int argc, char* argv[]) {
    int port = 8080;
    int ioThreads = 2;  // 默认 2 个 sub-reactor 线程（不含 main reactor）

    if (argc > 1) port = std::atoi(argv[1]);
    if (argc > 2) ioThreads = std::atoi(argv[2]);

    std::cout << "=== Multi-Reactor epoll server ===" << std::endl;
    std::cout << "Port:       " << port << std::endl;
    std::cout << "IO threads: " << ioThreads << " (sub-reactors)" << std::endl;
    std::cout << "==================================" << std::endl;

    // 创建主 Reactor 的 EventLoop
    EventLoop mainLoop;

    // 创建服务器（Acceptor 注册在 mainLoop）
    Server server(&mainLoop, port);
    server.setThreadNum(ioThreads);

    // 设置连接回调
    server.setConnectionCallback([](TcpConnection* conn) {
        std::cout << "[connection] " << conn->name()
                  << " handled by thread "
                  << std::this_thread::get_id()
                  << std::endl;
        conn->send("Welcome to Multi-Reactor echo server!\r\n");
    });

    // 设置消息回调 — echo 服务
    server.setMessageCallback([](TcpConnection* conn, Buffer* buf) {
        std::string msg = buf->retrieveAllAsString();

        // 去掉末尾换行仅用于日志显示
        std::string log = msg;
        while (!log.empty() && (log.back() == '\n' || log.back() == '\r')) {
            log.pop_back();
        }
        std::cout << "[message] from " << conn->name()
                  << " on thread " << std::this_thread::get_id()
                  << ": " << log << std::endl;

        // 回显
        conn->send(msg);
    });

    // 启动服务器（start 内部会启动 sub-reactor 线程池）
    server.start();

    // 主 Reactor 进入事件循环（mainLoop 在 main 线程运行）
    mainLoop.loop();

    return 0;
}
