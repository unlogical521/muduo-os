//
// 基于 Reactor 模式的 epoll 服务器（主入口）
//
// 用法：
//   ./server          ← 启动 echo 服务器（端口 8080）
//   ./server 9090     ← 启动 echo 服务器（端口 9090）
//
// 设计思路（Reactor 模式）：
//
//   ┌──────────────────────────────────────────────────┐
//   │                     Server                        │
//   │  ┌──────────┐   ┌──────────────────────────────┐  │
//   │  │ Acceptor │   │   TcpConnection 集合          │  │
//   │  │  (listen)│   │   ┌──────┐ ┌──────┐ ┌──────┐ │  │
//   │  │  ┌─────┐ │   │   │ conn │ │ conn │ │ conn │ │  │
//   │  │  │Chnl │ │   │   │┌───┐ │ │┌───┐ │ │┌───┐ │ │  │
//   │  │  └─────┘ │   │   ││Chnl││ ││Chnl││ ││Chnl││ │  │
//   │  └──────────┘   │   │└───┘ │ │└───┘ │ │└───┘ │ │  │
//   │                 │   └──────┘ └──────┘ └──────┘ │  │
//   │                 └──────────────────────────────┘  │
//   └──────────────────────────────────────────────────┘
//                          │ 事件分发（epoll_wait）
//                          ▼
//                   ┌──────────────┐
//                   │  EventLoop   │
//                   │  (epoll fd)  │
//                   └──────────────┘
//
//  1. EventLoop 封装 epoll，负责事件循环
//  2. Channel 封装一个 fd 及其回调
//  3. Acceptor 封装监听 fd，新连接到来时 accept
//  4. TcpConnection 封装一个已连接 fd，处理读写
//  5. Server 组装一切，对外暴露消息回调
//
#include <iostream>
#include <memory>
#include <cstring>
#include <unistd.h>

#include "EventLoop.h"
#include "Server.h"
#include "TcpConnection.h"
#include "Buffer.h"

int main(int argc, char* argv[]) {
    int port = 8080;
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }

    std::cout << "=== Reactor epoll server ===" << std::endl;
    std::cout << "Listening on port " << port << std::endl;
    std::cout << "============================" << std::endl;

    // 创建事件循环
    EventLoop loop;

    // 创建服务器
    Server server(&loop, port);

    // 设置连接回调（可选）
    server.setConnectionCallback([](TcpConnection* conn) {
        std::cout << "[connection] " << conn->name()
                  << " (fd=" << conn->fd() << ") connected" << std::endl;

        // 发送欢迎消息
        conn->send("Welcome to Reactor echo server!\r\n");
    });

    // 设置消息回调 — echo 服务
    server.setMessageCallback([](TcpConnection* conn, Buffer* buf) {
        std::string msg = buf->retrieveAllAsString();

        // 去掉末尾换行仅用于日志显示
        std::string log = msg;
        if (!log.empty() && (log.back() == '\n' || log.back() == '\r')) {
            log.pop_back();
        }
        if (!log.empty() && (log.back() == '\r' || log.back() == '\n')) {
            log.pop_back();
        }
        std::cout << "[message] from " << conn->name()
                  << ": " << log << std::endl;

        // 回显
        conn->send(msg);
    });

    // 启动服务器
    server.start();

    // 进入事件循环（永不返回，除非 quit）
    loop.loop();

    return 0;
}
