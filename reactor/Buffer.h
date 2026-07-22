//
// Buffer — 简单的应用层缓冲区
// 内部用 std::vector<char> 存储，支持自动扩容
// 提供读/写接口，供 TcpConnection 使用
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

    // 可读字节数
    size_t readableBytes() const { return write_index_ - read_index_; }

    // 可写字节数（剩余空间）
    size_t writableBytes() const { return buf_.size() - write_index_; }

    // 读指针位置（可读起始位置）
    const char* peek() const { return &buf_[read_index_]; }

    // 读取 len 字节后，移动读指针
    void retrieve(size_t len) {
        if (len < readableBytes()) {
            read_index_ += len;
        } else {
            retrieveAll();
        }
    }

    // 全部取出
    void retrieveAll() {
        read_index_ = 0;
        write_index_ = 0;
    }

    // 取出所有可读数据，转为 string
    std::string retrieveAllAsString() {
        return retrieveAsString(readableBytes());
    }

    std::string retrieveAsString(size_t len) {
        std::string result(peek(), len);
        retrieve(len);
        return result;
    }

    // 确保可写空间足够
    void ensureWritableBytes(size_t len) {
        if (writableBytes() < len) {
            // 扩容
            buf_.resize(write_index_ + len);
        }
    }

    // 写入数据
    void append(const char* data, size_t len) {
        ensureWritableBytes(len);
        std::copy(data, data + len, &buf_[write_index_]);
        write_index_ += len;
    }

    void append(const std::string& data) {
        append(data.data(), data.size());
    }

    // 写指针后移（直接从 fd 读入后使用）
    void hasWritten(size_t len) {
        write_index_ += len;
    }

    // 从 fd 读数据（返回 recv 结果）
    ssize_t readFd(int fd);

private:
    std::vector<char> buf_;
    size_t read_index_;
    size_t write_index_;
};
