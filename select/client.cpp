#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>

int main() {
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons(8080);

    if (connect(client_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Connection failed" << std::endl;
        return -1;
    }
    std::cout << "Connected to select-server, type messages to send (or 'quit' to exit):" << std::endl;

    while (true) {
        std::string message;
        std::cout << "> ";
        std::getline(std::cin, message);

        if (message == "quit") break;

        send(client_fd, message.c_str(), message.size(), 0);

        char buffer[1024] = {0};
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read > 0) {
            std::cout << "Echo: " << buffer << std::endl;
        } else if (bytes_read == 0) {
            std::cout << "Server closed the connection." << std::endl;
            break;
        } else {
            std::cerr << "Error reading from server" << std::endl;
            break;
        }
    }

    close(client_fd);
    return 0;
}
