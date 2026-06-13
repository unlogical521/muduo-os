#include <iostream>
#include <unistd.h>
#include <cstring>
#include <sys/types.h>

// 父子进程通过管道通信
int main(){
    // 创建管道
    int p_p2c[2];
    int p_c2p[2];
    // 创建两个管道
    if(pipe(p_p2c) == -1 || pipe(p_c2p) == -1){
        std::cerr << "Failed to create pipes" << std::endl;
        return 1;
    }
    if(fork()!=0){
        // 父进程
        // 0 读 1 写
        close(p_p2c[0]); // 关闭读端
        close(p_c2p[1]); // 关闭写端
        while(1)
        {
            std::string input;
            std::cout << "Enter a message to send to child:";
            std::getline(std::cin, input);
            if(input == "exit"){
                break;
            }
            const char* buf = input.c_str();
            if(write(p_p2c[1], buf, strlen(buf)) == -1){ // 向管道写入数据
                std::cerr << "Failed to write to pipe" << std::endl;
                break;
            }
            char child_response[4096];
            ssize_t n = read(p_c2p[0], child_response, sizeof(child_response) - 1);
            if(n > 0){ // 从管道读取子进程的响应
                child_response[n] = '\0';
                std::cout << "Child responded: " << child_response << std::endl;
            }
            
        }
        close(p_p2c[1]); // 关闭写端
        close(p_c2p[0]); // 关闭读端
    }
    else{
        // 子进程关闭关联不到的读写端
        close(p_p2c[1]);
        close(p_c2p[0]);
        char buf[4096];
        while(1)
        {
            memset(buf, 0, sizeof(buf));
            ssize_t n = read(p_p2c[0], buf, sizeof(buf) - 1);
            if(n > 0) { // 从管道读取数据
                buf[n] = '\0';
                std::cout << "Child received: " << buf << std::endl;
                std::string response;
                std::cout << "Enter a response to parent: ";
                std::getline(std::cin, response);
                if(write(p_c2p[1], response.c_str(), response.size()) == -1){ // 向管道写入响应
                    std::cerr << "Failed to write to pipe" << std::endl;
                    break;
                }
                if(strcmp(buf, "exit") == 0){
                    break;
                }
            } else {
                break; // read等于0说明写端已经关闭
            }
        }
        close(p_p2c[0]); // 关闭读端
        close(p_c2p[1]); // 关闭写端
    }
    return 0;
}