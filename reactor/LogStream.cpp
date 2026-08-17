//
// LogStream 实现（非模板 operator<< 部分）
//
#include "LogStream.h"
#include <cstring>

LogStream& LogStream::operator<<(bool v) {
    if (discard_) return *this;
    append(v ? "1" : "0", 1);
    return *this;
}

LogStream& LogStream::operator<<(double v) {
    if (discard_) return *this;
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%.6f", v);
    append(buf, len);
    return *this;
}

LogStream& LogStream::operator<<(float v) {
    if (discard_) return *this;
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%.6f", static_cast<double>(v));
    append(buf, len);
    return *this;
}

// 指针打印为十六进制 0x...
LogStream& LogStream::operator<<(const void* v) {
    if (discard_) return *this;
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%p", v);
    append(buf, len);
    return *this;
}

LogStream& LogStream::operator<<(char c) {
    if (discard_) return *this;
    buffer_.add(c);
    return *this;
}

LogStream& LogStream::operator<<(const char* s) {
    if (discard_) return *this;
    if (s == nullptr) {
        append("(null)", 6);
        return *this;
    }
    append(s, static_cast<int>(strlen(s)));
    return *this;
}

LogStream& LogStream::operator<<(const std::string& s) {
    if (discard_) return *this;
    append(s.data(), static_cast<int>(s.size()));
    return *this;
}

LogStream& LogStream::operator<<(const std::string_view& s) {
    if (discard_) return *this;
    append(s.data(), static_cast<int>(s.size()));
    return *this;
}
