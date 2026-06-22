// select-based server
// 使用 I/O 多路复用 select() 单线程处理多个客户端连接
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <algorithm>
#include <sys/select.h>

int main() {
    // 1. 创建监听 socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return -1;
    }

    // 设置端口复用，避免 "Address already in use"
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);

    if (bind(server_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, 5) < 0) {
        std::cerr << "Listen failed" << std::endl;
        close(server_fd);
        return -1;
    }

    std::cout << "Server is listening on port 8080 (select model)" << std::endl;

    // 2. 用 vector 保存所有已连接的客户端 fd
    std::vector<int> client_fds;

    int max_fd = server_fd;

    while (true) {
        // 3. 每次循环重新构造可读 fd_set
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);

        for (int fd : client_fds) {
            FD_SET(fd, &read_fds);
        }

        // 4. 调用 select —— 阻塞等待任意 fd 可读
        int activity = select(max_fd + 1, &read_fds, nullptr, nullptr, nullptr);
        if (activity < 0) {
            std::cerr << "select() error" << std::endl;
            break;
        }

        // 5. 处理监听 fd 上的新连接
        // 有新连接到来，accept 并加入 client_fds
        if (FD_ISSET(server_fd, &read_fds)) {
            sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int new_fd = accept(server_fd, (sockaddr*)&client_addr, &addr_len);
            if (new_fd >= 0) {
                std::cout << "New connection accepted, fd=" << new_fd << std::endl;
                client_fds.push_back(new_fd);
                if (new_fd > max_fd) {
                    max_fd = new_fd;
                }
            }
            // 如果只有新连接事件，继续下一轮 select
            if (--activity == 0) continue;
        }

        // 6. 遍历客户端 fd，处理已经就绪的读事件
        for (auto it = client_fds.begin(); it != client_fds.end(); ) {
            int fd = *it;
            if (FD_ISSET(fd, &read_fds)) {
                char buffer[1024] = {0};
                ssize_t bytes_read = recv(fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes_read > 0) {
                    // 收到数据，原样回显 (echo)
                    buffer[bytes_read] = '\0';
                    std::cout << "Received from fd=" << fd << ": " << buffer << std::endl;
                    send(fd, buffer, bytes_read, 0);
                    ++it;
                } else {
                    // bytes_read == 0 表示对方关闭；< 0 表示错误
                    if (bytes_read == 0) {
                        std::cout << "Client fd=" << fd << " disconnected" << std::endl;
                    } else {
                        std::cerr << "Error reading from fd=" << fd << std::endl;
                    }
                    close(fd);
                    it = client_fds.erase(it);
                }

                if (--activity == 0) break;
            } else {
                ++it;
            }
        }
    }

    // 清理
    for (int fd : client_fds) close(fd);
    close(server_fd);
    return 0;
}
