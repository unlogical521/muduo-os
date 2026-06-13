# 标准
cmake_minimum_required(VERSION 3.10)
# cpp
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
# 项目
project(muduo)
# 可执行文件
add_executable(server v1/server.cpp)
add_executable(client v1/client.cpp)
