//
// LogStream — 日志前端缓冲流（仿 muduo）
//
// 为什么需要它？
//   直接把数据格式化进 std::string 会频繁堆分配；每条日志都加锁（如 std::cerr）
//   在高并发多线程下是性能瓶颈。LogStream 用线程局部的固定大小栈缓冲，
//   日志调用只做纯内存写入，零堆分配、零锁竞争。
//
// 结构：
//   FixedBuffer<SIZE>  固定大小字节缓冲（栈上 char 数组）
//   LogStream          提供 operator<< 流式写入 + 自实现整数格式化
//
// 级别过滤：被过滤的日志通过 discard() 标记，operator<< 直接丢弃写入，
//           避免无效格式化开销。
//
#pragma once

#include <string>
#include <string_view>
#include <cstring>
#include <type_traits>
#include <cstdio>

namespace muduo_detail {

// 固定大小字节缓冲（模板，SIZE 编译期确定）
// data_：存储区；cur_：下一个可写位置。不自动扩容，写满即截断。
template <int SIZE>
class FixedBuffer {
public:
    FixedBuffer() : cur_(data_) {}
    ~FixedBuffer() = default;

    // 追加 len 字节（仅写满当前容量）
    void append(const char* buf, size_t len) {
        if (avail() > static_cast<int>(len)) {
            memcpy(cur_, buf, len);
            cur_ += len;
        }
    }

    // 追加单字节
    void add(char c) {
        if (avail() > 1) {
            *cur_ = c;
            ++cur_;
        }
    }

    // 已写入数据起始
    const char* data() const { return data_; }
    // 已写入字节数
    int length() const { return static_cast<int>(cur_ - data_); }
    // 剩余可写空间
    int avail() const { return static_cast<int>(end() - cur_); }

    // 重置（写指针归零，数据保留待覆盖）
    void reset() { cur_ = data_; }

private:
    const char* end() const { return data_ + sizeof(data_); }

    char data_[SIZE];
    char* cur_;
};

}  // namespace muduo_detail

class LogStream {
public:
    // 前端缓冲大小（日志行一般远小于 4KB）
    static const int kSmallBuffer = 4000;
    using Buffer = muduo_detail::FixedBuffer<kSmallBuffer>;

    LogStream() : discard_(false) {}

    // 标记本流丢弃后续写入（级别过滤用）
    void discard(bool d) { discard_ = d; }
    bool discard() const { return discard_; }

    const Buffer& buffer() const { return buffer_; }
    Buffer& buffer() { return buffer_; }

    void resetBuffer() { buffer_.reset(); }

    // ========== operator<< 全类型支持 ==========
    // 非模板类型在 LogStream.cpp 实现；整型模板内联（必须）

    // 整型：自实现十进制转换（无堆分配，临时数组反转法）
    // 相比 std::to_string / snprintf 避免了堆分配与格式化开销
    template <typename T>
    LogStream& formatInteger(T v) {
        if (discard_) return *this;
        char buf[32];   // 最大 20 位数字 + 负号
        char* p = buf + sizeof(buf);

        // 统一按无符号处理，负号单独记录
        using U = typename std::make_unsigned<T>::type;
        U u = static_cast<U>(v);
        bool negative = false;
        if (std::is_signed<T>::value && v < 0) {
            negative = true;
            u = static_cast<U>(0) - u;
        }

        do {
            *--p = static_cast<char>('0' + (u % 10));
            u /= 10;
        } while (u != 0);
        if (negative) {
            *--p = '-';
        }
        append(p, static_cast<int>(buf + sizeof(buf) - p));
        return *this;
    }

    // 所有整型 operator<< 走 formatInteger
    LogStream& operator<<(short v) { return formatInteger(v); }
    LogStream& operator<<(unsigned short v) { return formatInteger(v); }
    LogStream& operator<<(int v) { return formatInteger(v); }
    LogStream& operator<<(unsigned int v) { return formatInteger(v); }
    LogStream& operator<<(long v) { return formatInteger(v); }
    LogStream& operator<<(unsigned long v) { return formatInteger(v); }
    LogStream& operator<<(long long v) { return formatInteger(v); }
    LogStream& operator<<(unsigned long long v) { return formatInteger(v); }

    // 非模板类型（在 LogStream.cpp 实现）
    LogStream& operator<<(bool v);
    LogStream& operator<<(double v);
    LogStream& operator<<(float v);
    LogStream& operator<<(const void* v);
    LogStream& operator<<(char c);
    LogStream& operator<<(const char* s);
    LogStream& operator<<(const std::string& s);
    LogStream& operator<<(const std::string_view& s);

private:
    void append(const char* data, int len) { buffer_.append(data, len); }

    Buffer buffer_;
    bool discard_;
};
