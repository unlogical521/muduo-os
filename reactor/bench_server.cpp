//
// bench_server — 最小回显基准服务器（用于并发性能测试）
//
// 与 re_server(聊天室 demo) 的区别：
//   1. 消息回调 = 纯回显（echo），不做广播 —— 避免 O(N) 广播放大干扰吞吐测量
//   2. 不逐条打日志 —— 避免异步日志的磁盘/CPU 开销干扰测量
//   3. 无心跳定时器 —— 避免额外的定时扫描开销
//
// 目的：隔离出"多 Reactor 网络库本身"的并发伸缩性。
//
// 用法：
//   ./bench_server [port] [ioThreads]
//   默认 port=8080, ioThreads=2；ioThreads=0 表示单线程模式
//
#include <memory>
#include <cstring>
#include <unistd.h>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <atomic>

#include "EventLoop.h"
#include "Server.h"
#include "TcpConnection.h"
#include "Buffer.h"

EventLoop* g_mainLoop = nullptr;
std::atomic<bool> g_running{true};

static void signalHandler(int) {
    if (g_mainLoop) g_mainLoop->quit();
}

int main(int argc, char* argv[]) {
    int port = 8080;
    int ioThreads = 2;

    if (argc > 1) port = std::atoi(argv[1]);
    if (argc > 2) ioThreads = std::atoi(argv[2]);

    EventLoop mainLoop;
    g_mainLoop = &mainLoop;

    ::signal(SIGINT, signalHandler);
    ::signal(SIGTERM, signalHandler);

    Server server(&mainLoop, port);
    server.setThreadNum(ioThreads);

    // 回显：收到什么就原样发回去（多包时逐包回显，保证 RTT 测量精确）
    server.setMessageCallback([](TcpConnection* conn, Buffer* buf) {
        conn->send(buf->peek(), buf->readableBytes());
        buf->retrieveAll();
    });

    server.start();
    // 就绪标记写到 stdout，供测试脚本探测
    std::printf("BENCH_SERVER_READY port=%d ioThreads=%d\n", port, ioThreads);
    std::fflush(stdout);

    mainLoop.loop();
    std::printf("BENCH_SERVER_STOPPED\n");
    std::fflush(stdout);
    return 0;
}
