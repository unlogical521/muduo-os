# 标准
cmake_minimum_required(VERSION 3.10)
# cpp
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
# 项目
project(one2one)
# 可执行文件
add_executable(server oneThread2oneFd/server.cpp)
add_executable(client oneThread2oneFd/client.cpp)
