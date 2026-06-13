// server
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <cstring>

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);
    bind(server_fd, (sockaddr*)&address, sizeof(address));
    listen(server_fd, 3);
    std::cout << "Server is listening on port 8080" << std::endl;
    // 接受客户端连接
    // 服务器进入循环，等待客户端连接
    while (true) {
        int new_socket = accept(server_fd, nullptr, nullptr);
        if (new_socket >= 0) {
            std::cout << "Accepted a new connection from client:" << new_socket << std::endl;
        }
        // 维护一个连接池
        static std::vector<int> connections;
        connections.push_back(new_socket);
        // 处理客户端连接
        // 这里可以创建一个线程来处理每个客户端连接，或者使用非阻塞式I/O来处理多个连接
        // 例如，使用线程来处理每个客户端连接
        std::thread([new_socket]() {
            // 在这里处理客户端连接，例如读取数据、发送响应等
            // 持续收发
            char buffer[1024] = {0};
            while (true) {
                
                ssize_t bytes_read = recv(new_socket, buffer, sizeof(buffer), 0);
                if (bytes_read > 0) {
                    std::cout << "Received from client " << new_socket << ": " << buffer << std::endl;
                    // 键盘输入响应
                    std::string response;
                    std::cout << "Enter response to client " << new_socket << ": ";
                    std::getline(std::cin, response);
                    send(new_socket, response.c_str(), response.size(), 0);
                } else if (bytes_read == 0) {
                    std::cout << "Client " << new_socket << " disconnected." << std::endl;
                    close(new_socket);
                    break;
                } else {
                    std::cerr << "Error reading from client " << new_socket << std::endl;
                    close(new_socket);
                    break;
                }
            }
        }).detach();

    }   
    return 0;
}