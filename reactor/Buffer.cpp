//
// Buffer 实现 —— readFd 使用栈上额外空间 + scatter-gather IO
//
// 为什么不用 recv + 追加？
//   如果 buffer 的可写空间小于对方一次发送的数据，需要多次 recv。
//   使用 readv 可以将数据分散到两个内存块：
//     第一块：buffer 现有可写空间
//     第二块：栈上的 64KB 额外缓冲区
//   这样一次 readv 就能读走所有数据，无需多次系统调用。
//
#include "Buffer.h"
#include <sys/uio.h>
#include <unistd.h>

// readFd — 从 fd 读取数据到 Buffer
// 使用 readv 实现 scatter-gather IO：
//   vec[0] = Buffer 的可写区域
//   vec[1] = 栈上 64KB 额外空间
// 如果读入的数据超过 Buffer 剩余空间，超出的部分在 extrabuf 中，
// 再通过 append 追加入 Buffer。
//
// 返回值：读取的字节数，-1 表示错误
ssize_t Buffer::readFd(int fd) {
    // 栈上额外缓冲区（避免单次 Buffer 太小导致多次 readv 调用）
    char extrabuf[65536];
    struct iovec vec[2];

    // 第一块：当前 Buffer 的可写区域
    vec[0].iov_base = &buf_[write_index_];
    vec[0].iov_len  = writableBytes();

    // 第二块：栈上额外缓冲区（足够大，基本保证一次读完）
    vec[1].iov_base = extrabuf;
    vec[1].iov_len  = sizeof(extrabuf);

    constexpr int iovcnt = 2;
    ssize_t n = ::readv(fd, vec, iovcnt);
    if (n < 0) {
        return n;
    }

    // 如果数据填进了第一块（Buffer 的可写空间够大），更新 write_index_
    size_t written_to_first = std::min(static_cast<size_t>(n), writableBytes());
    write_index_ += written_to_first;

    // 剩余数据在 extrabuf 中，追加到 Buffer
    size_t remaining = n - written_to_first;
    if (remaining > 0) {
        append(extrabuf, remaining);
    }

    return n;
}
