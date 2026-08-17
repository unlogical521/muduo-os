//
// AsyncLogging 实现
//
#include "AsyncLogging.h"

#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <chrono>

// 前端堆积缓冲数超过该阈值时丢弃最旧的（防止内存无限增长）
namespace {
const int kMaxBuffers = 25;
}

AsyncLogging::AsyncLogging(const std::string& basename,
                           int rollSize,
                           int flushInterval)
    : basename_(basename),
      rollSize_(rollSize),
      flushInterval_(flushInterval),
      running_(false),
      currentBuffer_(new Buffer),
      nextBuffer_(new Buffer),
      file_(nullptr),
      writtenBytes_(0) {}

AsyncLogging::~AsyncLogging() {
    stop();
    // 覆盖"从未 start"的情况：后端线程没开过文件，这里兜底关闭
    if (file_) {
        fclose(file_);
        file_ = nullptr;
    }
}

// ========== 前端入口（任意线程调用） ==========
// 写满才加锁交接 + notify；未满时无锁等待，后端靠 wait_for 超时兜底 flush
void AsyncLogging::append(const char* msg, int len) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (currentBuffer_->avail() > len) {
        currentBuffer_->append(msg, len);
    } else {
        // 当前缓冲写满 → 交出去，换备用缓冲继续写
        buffers_.push_back(std::move(currentBuffer_));
        if (nextBuffer_) {
            currentBuffer_ = std::move(nextBuffer_);
        } else {
            currentBuffer_.reset(new Buffer);
        }
        currentBuffer_->append(msg, len);
        cond_.notify_one();
    }
}

// ========== 启动 / 停止 ==========
void AsyncLogging::start() {
    running_ = true;
    openFile();   // 先打开文件，后端线程才能写
    thread_ = std::thread(&AsyncLogging::threadFunc, this);
}

void AsyncLogging::stop() {
    if (!running_.exchange(false)) {
        return;   // 从未 start 或已 stop
    }
    cond_.notify_one();   // 打断 wait_for，让后端线程立即退出
    if (thread_.joinable()) {
        thread_.join();   // 等待落盘完成
    }
}

// 请求尽快落盘：唤醒后端线程立即取走当前缓冲写盘（无需等待 flushInterval）
void AsyncLogging::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    cond_.notify_one();
}

// ========== 后端线程 ==========
void AsyncLogging::threadFunc() {
    // 后端持有的两个归还缓冲
    BufferPtr newBuffer1(new Buffer);
    BufferPtr newBuffer2(new Buffer);
    BufferVector buffersToWrite;

    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (buffers_.empty()) {
                // 等待前端填充；超时也返回（定期 flush 兜底，保证日志及时落盘）
                cond_.wait_for(lock, std::chrono::seconds(flushInterval_));
            }

            // 把前端当前缓冲交给写盘队列，换走后端归还的缓冲
            buffers_.push_back(std::move(currentBuffer_));
            currentBuffer_ = std::move(newBuffer1);
            buffers_.swap(buffersToWrite);
            if (!nextBuffer_) {
                nextBuffer_ = std::move(newBuffer2);
            }
        }

        // 前端积压过多（生产者远快于消费者）→ 丢弃最旧，只保留最近两条
        if (buffersToWrite.size() > kMaxBuffers) {
            fwrite("[AsyncLogging] drop old log buffers\n", 1, 36, stderr);
            buffersToWrite.erase(
                buffersToWrite.begin(),
                buffersToWrite.begin() + buffersToWrite.size() - 2);
        }

        // 无锁写盘（磁盘 IO 只发生在本线程，不阻塞任何网络线程）
        for (const auto& buf : buffersToWrite) {
            writeToFile(buf->data(), buf->length());
        }
        flushFile();

        // 归还缓冲给前端复用
        // 注意1：buffersToWrite 至少 1 个（循环里已 push currentBuffer_），
        //   但可能恰好 1 个（首次 flush 超时、期间无日志）——此时第二次 back() 会越界空 vector，
        //   必须检查后补一个新缓冲，而不是盲取两个。
        // 注意2：归还前必须 reset()。wait_for 超时会交换"未满"的 currentBuffer_，
        //   归还的缓冲里残留旧数据，若前端直接 append 新日志会拼在旧数据后面 → 日志重复。
        newBuffer1 = std::move(buffersToWrite.back());
        newBuffer1->reset();
        buffersToWrite.pop_back();
        if (!buffersToWrite.empty()) {
            newBuffer2 = std::move(buffersToWrite.back());
            newBuffer2->reset();
            buffersToWrite.pop_back();
        } else {
            newBuffer2.reset(new Buffer);
        }
    }

    // 退出前把前端残留缓冲（currentBuffer_ 中尚未写盘的日志）落盘
    {
        std::unique_lock<std::mutex> lock(mutex_);
        buffers_.push_back(std::move(currentBuffer_));
        buffers_.swap(buffersToWrite);
    }
    for (const auto& buf : buffersToWrite) {
        writeToFile(buf->data(), buf->length());
    }
    flushFile();

    if (file_) {
        fclose(file_);
        file_ = nullptr;
    }
}

// ========== 文件管理 ==========
// 文件名：basename.yyyymmdd-HHMMSS.pid.log
void AsyncLogging::openFile() {
    time_t t = time(nullptr);
    struct tm tmv;
    localtime_r(&t, &tmv);

    char filename[256];
    int n = snprintf(filename, sizeof(filename),
                     "%s.%04d%02d%02d-%02d%02d%02d.%d.log",
                     basename_.c_str(),
                     tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                     tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
                     static_cast<int>(getpid()));
    (void)n;

    file_ = fopen(filename, "a");   // 追加模式
    writtenBytes_ = 0;
    if (!file_) {
        fprintf(stderr, "AsyncLogging: open log file %s failed\n", filename);
    }
}

// 写盘；超过 rollSize_ 后滚动到新文件
void AsyncLogging::writeToFile(const char* data, int len) {
    if (!file_) return;
    size_t written = fwrite(data, 1, len, file_);
    writtenBytes_ += static_cast<long long>(written);
    if (writtenBytes_ > rollSize_) {
        fclose(file_);
        file_ = nullptr;
        openFile();   // 滚动：按新时间戳重开
    }
}
