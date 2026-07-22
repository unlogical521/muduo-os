cmake_minimum_required(VERSION 3.10)
project(reactor_server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# reactor 库（静态编译）
add_library(reactor
    EventLoop.cpp
    Channel.cpp
    Acceptor.cpp
    TcpConnection.cpp
    Buffer.cpp
    Server.cpp
)

# 服务器可执行文件
add_executable(re_server server.cpp)
target_link_libraries(re_server reactor)

# 客户端可执行文件
add_executable(re_client client.cpp)
target_link_libraries(re_client reactor)
