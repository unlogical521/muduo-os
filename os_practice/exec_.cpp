#include <iostream>
#include <unistd.h>
#include <cstring>
#include <sys/wait.h>
int main(){
    pid_t pid = fork();
    if(pid == -1){
        std::cerr << "Failed to fork" << std::endl;
        return 1;
    }
    if(pid == 0){
        // 子进程
        std::cout << "before exec" << std::endl;
        // 执行新的程序
        // exec系列函数会用新的程序替换当前进程的映像，执行新的程序代码
        // 只有当exec失败时才会返回，成功则不会返回
        // execv函数需要指定路径和参数列表，参数列表必须以NULL结尾
        char* args[] = {(char*)"ls", (char*)"-l", (char*)"/home/illogical/project/muduo/os_practice", nullptr};
        if(execv("/bin/ls", args) == -1){
            std::cerr << "Failed to execute ls" << std::endl;
            return 1;
        }
        std::cout << "after exec" << std::endl; // 不会被执行到
        return 0;
    } else {
        // 父进程
        wait(nullptr); // 等待子进程结束
    }
    return 0;   
}