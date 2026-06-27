#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <algorithm>
#include <sys/select.h>

int main(){
    //
    int l_fd = socket(AF_INET,SOCK_STREAM,0);
    if (l_fd < 0)
    {
        std::cerr << "fail to create socket" << std::endl;
        return -1;
    }
    // 端口复用
    // TCP连接 断开连接的一方会等待2MSL linux设定这个时间为60s
    // 服务器因维护重启时，必须等待
    int opt = 1;
    setsockopt(l_fd,SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    // 绑定地址
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port  = htons(8080);

    if(bind(l_fd,(sockaddr*)&addr,sizeof(addr))<0){
        std::cerr << "fail to bind" << std::endl;
        close(l_fd);
        return -1;
    }
    // 监听
    if(listen(l_fd,5)<0){
        std::cerr << "fail to listen" << std::endl;
        close(l_fd);
        return -1;
    }
    std::cout << "Server is listening on port 8080 (select model)" << std::endl;
    // 保存所有关注的通信套接字
    std::vector<int> cfds;
    // select函数第一个参数 关注的fd列表中最大的+1
    int max_fd = l_fd;
    while(1){
        fd_set read_fds;
        // 清空
        FD_ZERO(&read_fds);
        // 关注通信套接字的读事件
        FD_SET(l_fd,&read_fds);
        for(int fd : cfds){
            FD_SET(fd,&read_fds);
        }
        // 看看有无事件
        // 无限等待，直到有事件发生
        // 有数据->网卡中断->cpu设置DMA参数->DMA将数据搬运到内核缓冲区->完成后触发硬中断->cpu处理
        // ->唤醒网络相关线程->数据处理->sock接收队列->唤醒阻塞在select的进程
        int activity = select(max_fd+1,&read_fds,nullptr,nullptr,nullptr);
        if(activity<0){
            std::cerr << "select failed" << std::endl;
            break;
        }
        // 看看有没有新到访的客人要登记
        if(FD_ISSET(l_fd,&read_fds)){
            sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int new_fd = accept(l_fd,(sockaddr*)&client_addr,&addr_len);
            // if(new_fd<0){
            //     std::cerr << "fail to accept new client" << std::endl;
            //     break;
            // }

            // 建立新链接后
            // 加入通信套接字数组
            if(new_fd>=0){
                cfds.push_back(new_fd);
                // 更改参数
                if(new_fd > max_fd){
                    max_fd = new_fd;
                }
            }

            // 如果只有这一个事件，直接跳过
            if(--activity <= 0)continue;
        }
        // 遍历所有通信fd，看看有没有事件
        for(auto it = cfds.begin();it != cfds.end();){
            int c_fd = * it;
            if(FD_ISSET(c_fd,&read_fds)){
                // 接收信息
                char buf[1024] = {0};
                size_t bytes_read = recv(c_fd,buf,sizeof(buf)-1,0);
                if(bytes_read > 0){
                    // 以 0 作为结尾
                    buf[bytes_read] = '0';
                    send(c_fd,buf,sizeof(buf),0);
                    ++it;
                }
                else{
                    // bytes_read == 0
                    // 连接关闭
                    if(bytes_read == 0){
                        std::cout << "client fd=%d disconnected" << c_fd << std::endl;
                    }
                    else{
                        std::cerr << "received from client fd = " << c_fd << "failed" <<std::endl;
                    }
                    close(c_fd);
                    it = cfds.erase(it);   
                }
                if(--activity == 0)break;
            }
            else{
                ++it;
            }
        }
    }
    // 通信结束，关掉所有套接字
    for(int c_fd : cfds)close(c_fd);
    close(l_fd);
    return 0;
}