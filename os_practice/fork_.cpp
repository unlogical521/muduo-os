#include <unistd.h>
#include <iostream>

int main(){
    std::cout << "before fork" << std::endl;
    // 复制子进程
    // 子进程与父进程映射相同物理内存，在fork之后，执行相同的逻辑
    // 遵循 “读时共享 写时复制” 原则
    int i;
    // 创建五个子进程
    for(i=0;i<5;i++){
        if(fork()==0){
            break;
        }
    }
    // 创建完了
    // 只有父进程会执行最后一步自增
    if(i==5){
        printf("i am parent\n");
    }
    else{
        printf("i am %dth child\n",i+1);
    }
    return 0;

}