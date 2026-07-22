//
// Buffer 实现 —— readFd 使用栈上额外空间+scatter-gather IO
//
#include "Buffer.h"
#include <sys/uio.h>
#include <unistd.h>

ssize_t Buffer::readFd(int fd) {
    // 栈上额外缓冲区（避免单次 buffer 太小导致多次系统调用）
    char extrabuf[65536];
    struct iovec vec[2];

    // 第一块：当前 buffer 的可写区域
    vec[0].iov_base = &buf_[write_index_];
    vec[0].iov_len  = writableBytes();

    // 第二块：栈上额外缓冲区
    vec[1].iov_base = extrabuf;
    vec[1].iov_len  = sizeof(extrabuf);

    constexpr int iovcnt = 2;
    ssize_t n = ::readv(fd, vec, iovcnt);
    if (n < 0) {
        return n;
    }

    // 如果数据填进了第一块，更新 write_index_
    size_t written_to_first = std::min(static_cast<size_t>(n), writableBytes());
    write_index_ += written_to_first;

    // 剩余数据在 extrabuf 中，追加入 buffer
    size_t remaining = n - written_to_first;
    if (remaining > 0) {
        append(extrabuf, remaining);
    }

    return n;
}
