// Performance benchmark client for muduo server models
// 测量 RPS（吞吐量）和延迟百分位（P50/P95/P99）
//
// 编译: g++ -std=c++17 -O2 -lpthread perf_client.cpp -o perf_client
// 使用: ./perf_client [并发数] [每连接消息数] [消息大小]
//
// 示例:
//   ./perf_client 10 500 64    # 10 并发, 每连接 500 条, 每条 64 字节
//   ./perf_client 50 200 256   # 50 并发, 每连接 200 条, 每条 256 字节
//
// 典型用例（对比两个模型）:
//   终端 1: ./server          (跑 select 或 one2one 的 server)
//   终端 2: ./perf_client 10 500 64

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <string>
#include <numeric>
#include <cmath>

using Clock = std::chrono::high_resolution_clock;

// ======================== 统计 ========================

struct Stats {
    size_t  count;
    double  avg_us;
    double  min_us;
    double  max_us;
    double  p50_us;
    double  p95_us;
    double  p99_us;
};

Stats calc_stats(std::vector<double>& us) {
    if (us.empty()) return {0, 0, 0, 0, 0, 0, 0};
    std::sort(us.begin(), us.end());
    size_t n = us.size();
    double sum = std::accumulate(us.begin(), us.end(), 0.0);
    auto p = [&](double q) -> double {
        size_t idx = static_cast<size_t>(std::round(q * (n - 1)));
        return us[idx];
    };
    return {n, sum / n, us.front(), us.back(), p(0.50), p(0.95), p(0.99)};
}

// ======================== TCP 连接 ========================

int tcp_connect() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(8080);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// ======================== 单连接压测 ========================
// 返回每条消息的往返延迟（微秒）

std::vector<double> bench_connection(int msgs, int msg_size) {
    int fd = tcp_connect();
    if (fd < 0) return {};

    std::string payload(msg_size, 'x');
    auto* buf = new char[msg_size];
    std::vector<double> latencies;
    latencies.reserve(msgs);

    for (int i = 0; i < msgs; ++i) {
        auto t1 = Clock::now();

        // 发送
        ssize_t sent = send(fd, payload.data(), payload.size(), 0);
        if (sent <= 0) break;

        // 阻塞接收完整回显（与 msg_size 等长）
        ssize_t total_recv = 0;
        while (total_recv < msg_size) {
            ssize_t r = recv(fd, buf + total_recv, msg_size - total_recv, 0);
            if (r <= 0) goto cleanup;
            total_recv += r;
        }

        auto t2 = Clock::now();
        latencies.push_back(std::chrono::duration<double, std::micro>(t2 - t1).count());
    }

cleanup:
    delete[] buf;
    close(fd);
    return latencies;
}

// ======================== 打印结果 ========================

void print_result(const std::string& title,
                  int connections, int msgs_per_conn, int msg_size,
                  double elapsed_s,
                  const std::vector<double>& all_latencies) {

    Stats s = calc_stats(const_cast<std::vector<double>&>(all_latencies));

    std::cout << "\n============================================\n";
    std::cout << "  " << title << "\n";
    std::cout << "============================================\n";
    std::cout << "  并发连接数:      " << connections << "\n";
    std::cout << "  每连接消息数:    " << msgs_per_conn << "\n";
    std::cout << "  消息大小:        " << msg_size << " bytes\n";
    std::cout << "  成功请求数:      " << s.count << "\n";
    std::cout << "  总耗时:          " << std::fixed << std::setprecision(2) << elapsed_s << " s\n";
    std::cout << "--------------------------------------------\n";
    std::cout << "  Throughput\n";
    std::cout << "  RPS:             " << std::fixed << std::setprecision(0) << s.count / elapsed_s << " req/s\n";
    std::cout << "  带宽:            " << std::fixed << std::setprecision(2)
              << (s.count * msg_size) / (elapsed_s * 1024 * 1024) << " MB/s\n";
    std::cout << "--------------------------------------------\n";
    std::cout << "  Latency (us)\n";
    std::cout << "  avg:             " << std::fixed << std::setprecision(1) << s.avg_us << " us\n";
    std::cout << "  min:             " << s.min_us << " us\n";
    std::cout << "  max:             " << s.max_us << " us\n";
    std::cout << "  P50:             " << s.p50_us << " us\n";
    std::cout << "  P95:             " << s.p95_us << " us\n";
    std::cout << "  P99:             " << s.p99_us << " us\n";
    std::cout << "============================================\n\n";
}

// ======================== CSV 行输出 ========================

void print_csv(const std::string& model,
               int connections, int msgs_per_conn, int msg_size,
               double elapsed_s,
               const std::vector<double>& all_latencies) {
    Stats s = calc_stats(const_cast<std::vector<double>&>(all_latencies));
    // header: model,connections,msgs_per_conn,msg_size,elapsed_s,rps,bandwidth_mbps,avg_us,min_us,max_us,p50,p95,p99,count
    std::cout << model << ","
              << connections << ","
              << msgs_per_conn << ","
              << msg_size << ","
              << std::fixed << std::setprecision(3) << elapsed_s << ","
              << std::setprecision(0) << s.count / elapsed_s << ","
              << std::setprecision(2) << (s.count * msg_size) / (elapsed_s * 1024 * 1024) << ","
              << std::setprecision(1) << s.avg_us << ","
              << s.min_us << ","
              << s.max_us << ","
              << s.p50_us << ","
              << s.p95_us << ","
              << s.p99_us << ","
              << s.count << "\n";
}

// ======================== 主流程 ========================

int main(int argc, char* argv[]) {
    // 默认参数
    int connections   = 10;
    int msgs_per_conn = 500;
    int msg_size      = 64;

    if (argc >= 2) connections   = std::stoi(argv[1]);
    if (argc >= 3) msgs_per_conn = std::stoi(argv[2]);
    if (argc >= 4) msg_size      = std::stoi(argv[3]);

    std::cout << "===== 服务器性能压测 =====" << "\n";
    std::cout << "并发连接数:   " << connections << "\n";
    std::cout << "每连接消息数: " << msgs_per_conn << "\n";
    std::cout << "消息大小:     " << msg_size << " bytes\n";
    std::cout << "目标地址:     127.0.0.1:8080\n";
    std::cout << "（请确保 server 已启动）\n\n";

    // 先检查 server 是否可达
    int probe = tcp_connect();
    if (probe < 0) {
        std::cerr << "[错误] 无法连接 127.0.0.1:8080，请先启动 server！\n";
        return 1;
    }
    close(probe);
    std::cout << "Server 已就绪，开始压测...\n";

    // 启动并发连接
    auto start = Clock::now();

    std::vector<std::thread> threads;
    threads.reserve(connections);
    std::vector<std::vector<double>> results(connections);

    for (int i = 0; i < connections; ++i) {
        threads.emplace_back([i, msgs_per_conn, msg_size, &results]() {
            results[i] = bench_connection(msgs_per_conn, msg_size);
        });
    }
    for (auto& t : threads) t.join();

    auto end = Clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // 汇总延迟
    std::vector<double> all;
    size_t ok_total = 0;
    for (auto& v : results) {
        ok_total += v.size();
        all.insert(all.end(), v.begin(), v.end());
    }

    // 报告
    print_result("Benchmark Result", connections, msgs_per_conn, msg_size, elapsed, all);

    // CSV 单行输出（便于重定向收集数据）
    std::cout << "# CSV:\n";
    // 列头
    std::cout << "# model,connections,msgs_per_conn,msg_size,elapsed_s,rps,bandwidth_mbps,avg_us,min_us,max_us,p50,p95,p99,count\n";
    print_csv("server", connections, msgs_per_conn, msg_size, elapsed, all);

    // 失败提示
    size_t expected = static_cast<size_t>(connections) * msgs_per_conn;
    if (ok_total < expected) {
        std::cerr << "[警告] 预期 " << expected << " 次请求，实际成功 "
                  << ok_total << " 次（丢失 " << (expected - ok_total) << " 次）\n"
                  << "        消息大小过大或 server 处理能力不足。\n";
    }

    return 0;
}
