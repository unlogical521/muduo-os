//
// epoll 模型 — 单线程多路复用 echo 服务器
// 与 select 不同，epoll 不遍历所有 fd，只返回有事件的 fd，复杂度 O(1)
// 适用于处理大量并发连接（成千上万）
//
#include <iostream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <vector>

int main() {
    // 1. 创建监听 socket
    int l_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (l_fd < 0) {
        std::cerr << "fail to create socket" << std::endl;
        return -1;
    }

    // 端口复用 —— 避免 TIME_WAIT 状态下重启失败
    int opt = 1;
    setsockopt(l_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 2. 绑定地址
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);

    if (bind(l_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "fail to bind" << std::endl;
        close(l_fd);
        return -1;
    }

    // 3. 监听
    if (listen(l_fd, 5) < 0) {
        std::cerr << "fail to listen" << std::endl;
        close(l_fd);
        return -1;
    }
    std::cout << "Server is listening on port 8080 (epoll model)" << std::endl;

    // 4. 创建 epoll 实例
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        std::cerr << "fail to create epoll fd" << std::endl;
        close(l_fd);
        return -1;
    }

    // 5. 将监听 fd 加入 epoll 兴趣列表，关注可读事件（有新连接到来）
    epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = l_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, l_fd, &ev) < 0) {
        std::cerr << "fail to add listen fd to epoll" << std::endl;
        close(epfd);
        close(l_fd);
        return -1;
    }

    // 事件数组 —— 每次 epoll_wait 返回后从这里取事件
    std::vector<epoll_event> events(64);

    while (1) {
        // 6. 等待事件发生（无限超时）
        int nfds = epoll_wait(epfd, events.data(), events.size(), -1);
        if (nfds < 0) {
            std::cerr << "epoll_wait failed" << std::endl;
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            // 有事件发生的 fd 和事件类型
            int fd = events[i].data.fd;
            uint32_t evt = events[i].events;

            // 异常检查
            if (evt & (EPOLLERR | EPOLLHUP)) {
                std::cerr << "epoll error/hup on fd=" << fd << std::endl;
                close(fd);
                continue;
            }

            // === 监听 fd 可读 → 新连接 ===
            if (fd == l_fd) {
                sockaddr_in client_addr;
                socklen_t addr_len = sizeof(client_addr);
                int new_fd = accept(l_fd, (sockaddr*)&client_addr, &addr_len);
                if (new_fd >= 0) {
                    std::cout << "new client connected, fd=" << new_fd << std::endl;

                    // 将新连接 fd 加入 epoll，关注可读事件（默认水平触发 LT）
                    ev.events = EPOLLIN;
                    ev.data.fd = new_fd;
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, new_fd, &ev) < 0) {
                        std::cerr << "fail to add client fd to epoll" << std::endl;
                        close(new_fd);
                    }
                }
            }
            // === 通信 fd 可读 → 收到数据 ===
            else {
                char buf[1024] = {0};
                ssize_t bytes_read = recv(fd, buf, sizeof(buf) - 1, 0);

                if (bytes_read > 0) {
                    // 回声 —— 原样返回
                    send(fd, buf, bytes_read, 0);
                } else {
                    // bytes_read == 0 表示对方关闭；< 0 表示出错
                    if (bytes_read == 0) {
                        std::cout << "client fd=" << fd << " disconnected" << std::endl;
                    } else {
                        std::cerr << "recv from fd=" << fd << " failed" << std::endl;
                    }
                    // 从 epoll 移除并关闭
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }
        }
    }

    close(epfd);
    close(l_fd);
    return 0;
}
