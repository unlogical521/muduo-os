//
// Buffer — 简单的应用层缓冲区
// 内部用 std::vector<char> 存储，支持自动扩容
// 提供读/写接口，供 TcpConnection 使用
//
// 设计思路：
//   内部维护两个指针（read_index_, write_index_）而不是每次都 resize，
//   避免频繁的内存分配。当数据被 consume 后，指针后移，空间后续被复用。
//   只有当可用空间不足时才触发扩容。
//
#pragma once

#include <vector>
#include <string>
#include <algorithm>
#include <cstring>

class Buffer {
public:
    explicit Buffer(size_t initial_size = 1024)
        : buf_(initial_size), read_index_(0), write_index_(0) {}

    // 可读字节数（read_index_ 到 write_index_ 之间的距离）
    size_t readableBytes() const { return write_index_ - read_index_; }

    // 可写字节数（剩余空间，buf_.size() - write_index_）
    size_t writableBytes() const { return buf_.size() - write_index_; }

    // 读指针位置（可读起始位置）
    const char* peek() const { return &buf_[read_index_]; }

    // 读取 len 字节后，移动读指针
    // 如果 len 小于可读长度，只移动指针；否则重置两个指针（回收空间）
    void retrieve(size_t len) {
        if (len < readableBytes()) {
            read_index_ += len;
        } else {
            retrieveAll();
        }
    }

    // 全部取出——重置读/写指针，腾出全部空间
    void retrieveAll() {
        read_index_ = 0;
        write_index_ = 0;
    }

    // 取出所有可读数据，转为 string
    std::string retrieveAllAsString() {
        return retrieveAsString(readableBytes());
    }

    // 取出前 len 字节
    std::string retrieveAsString(size_t len) {
        std::string result(peek(), len);
        retrieve(len);
        return result;
    }

    // 确保可写空间足够
    // 如果剩余空间不够，直接扩容到 write_index_ + len
    void ensureWritableBytes(size_t len) {
        if (writableBytes() < len) {
            buf_.resize(write_index_ + len);
        }
    }

    // 写入数据（确保空间后 copy 进去）
    void append(const char* data, size_t len) {
        ensureWritableBytes(len);
        std::copy(data, data + len, &buf_[write_index_]);
        write_index_ += len;
    }

    void append(const std::string& data) {
        append(data.data(), data.size());
    }

    // 写指针后移（直接从 fd 读入后使用，配合 readv 的 scatter-gather）
    void hasWritten(size_t len) {
        write_index_ += len;
    }

    // 从 fd 读数据（内部使用 readv scatter-gather IO，减少数据拷贝）
    ssize_t readFd(int fd);

private:
    std::vector<char> buf_;
    size_t read_index_;    // 可读数据起始位置
    size_t write_index_;   // 可写空间起始位置
};
