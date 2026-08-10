# muduo — 从零实现的多 Reactor 网络库

> 参照陈硕《Linux 多线程服务端编程》中 muduo 的设计思想，**不依赖任何第三方网络库**，从零实现的一套多线程 TCP 网络框架。
>
> 本仓库按学习路径演进：`select` → `epoll` → 每线程一连接 → 多 Reactor，最终版本为 `reactor/`。本文档以 `reactor/` 为主体展开。

## 特性总览

| 能力 | 实现方式 |
|---|---|
| IO 多路复用 | epoll（LT 水平触发）+ `data.ptr` 直接定位 Channel |
| 多线程模型 | 主从 Reactor，`one loop per thread`，连接级 IO 无锁 |
| 跨线程调度 | `runInLoop` / `queueInLoop` + eventfd 唤醒 |
| 数据收发 | `readv` 分散读（栈上 64KB 兜底）、直写优先 + 缓冲兜底 |
| 定时器 | timerfd + 红黑树，与 epoll 统一事件循环、无独立定时线程 |
| 生命周期 | `shared_ptr` + 状态机 + 延迟关闭，杜绝悬垂与栈内析构 |
| 心跳回收 | 定时扫描连接表，自动强制关闭半开连接 |

## 架构总览（多 Reactor）

```
                 Main Reactor (main thread)
                 ┌───────────────────────────────┐
                 │  EventLoop  epoll (epfd)       │
                 │  ├─ Acceptor Channel (listen fd)│
                 │  │     accept → getNextLoop()   │
                 │  └─ wakeup Channel (eventfd)    │
                 └───────────────┬───────────────┘
                                 │ runInLoop() + eventfd 唤醒
                   ┌─────────────┼─────────────┐
                   ▼             ▼             ▼
        Sub Reactor[0]    Sub Reactor[1]    Sub Reactor[N]   （EventLoopThread 线程池）
        ┌───────────┐    ┌───────────┐    ┌───────────┐
        │ epoll     │    │ epoll     │    │ epoll     │
        │ ├ wakeup  │    │ ├ wakeup  │    │ ├ wakeup  │
        │ ├ timerfd │    │ ├ timerfd │    │ ├ timerfd │
        │ ├ conn-1  │    │ ├ conn-2  │    │ └ conn-3  │
        │ └ conn-4  │    │ └ conn-5  │    │           │
        └───────────┘    └───────────┘    └───────────┘
```

- **主 Reactor**（main 线程）：唯一持有监听 fd，负责 `accept`，通过 round-robin 把新连接分发给某个 Sub Reactor。
- **Sub Reactor**（线程池，默认 2 个）：各持独立 epoll 实例，只处理自己负责的连接的 IO，线程间互不竞争。连接创建后，**其所有读写、关闭回调都固定在一个 Sub Reactor 线程上执行，无锁**。
- 跨线程的唯一入口是 `runInLoop`（eventfd 唤醒），保证任何回调都只在自己的 loop 线程上跑。

## 核心设计（每一条对应一个底层考点）

### 1. 事件循环与 Channel 抽象

每个 fd 用一个 `Channel` 封装，持"感兴趣事件 `events_`"与"实际发生事件 `revents_`"，以及 4 个回调（读/写/关/错）。epoll 注册时用 `data.ptr` 直接存 `Channel*`（`EventLoop.cpp:107`），`epoll_wait` 返回后**零查找**派发到对应 Channel：

```
epoll_wait → events_[i].data.ptr = Channel* → channel->setRevents() → channel->handleEvent()
```

`handleEvent()` 按 `EPOLLERR > EPOLLIN/RDHUP > EPOLLOUT > EPOLLHUP` 优先级分发到具体回调。

### 2. 多 Reactor 与连接分发

`EventLoopThread` 封装"线程 + 专属 EventLoop"，`startLoop()` 用条件变量同步等待线程内 loop 构造完成（`EventLoopThread.cpp:30`）。`EventLoopThreadPool` 维护线程池，`getNextLoop()` 做 round-robin 负载均衡（`EventLoopThreadPool.cpp:58`）。

**为什么连接必须在 Sub Reactor 线程上创建？**
`TcpConnection` 构造时 `enableReading()` 会调用 `epoll_ctl` 把 connfd 加入所属 Sub Reactor 的 epoll 实例——这个操作必须在持有该 epoll 实例的线程上做。因此 `Server::onNewConnection` 通过 `ioLoop->runInLoop(...)` 把创建动作调度到目标线程（`Server.cpp:62`）。

### 3. 跨线程调度：runInLoop / queueInLoop + eventfd

`EventLoop` 内置一个 eventfd `wakeupFd_`，注册为 wakeup Channel。跨线程投递回调时：

1. `queueInLoop`：回调入队 `pendingFunctors_`（互斥锁保护）
2. 写 eventfd（内核计数器 +1）→ 目标线程的 `epoll_wait` 立即返回
3. 目标线程读走 eventfd → `doPendingFunctors()` 批量执行队列

**两个关键细节**：
- `doPendingFunctors` 先把整个队列 `swap` 到局部变量再执行，**锁只持有在 swap 的瞬间**，执行期间允许其它线程继续追加（`EventLoop.cpp:195`）。
- `callingPendingFunctors_` 标志处理竞态：若新任务在"队列已 swap 空但还没执行完"时入队，epoll 可能读不到已消费的唤醒信号，该标志会让新任务在**当前这一批的末尾再执行一次**，避免等下一轮 epoll_wait（`EventLoop.cpp:162`）。

### 4. 高性能收发

**读：** `Buffer::readFd` 用 `readv` 分散读——第一块是 Buffer 剩余空间，第二块是栈上 64KB 扩展缓冲，一次系统调用读尽所有数据，避免小 Buffer 导致的多次 recv（`Buffer.cpp:23`）。

**写：** `sendInLoop` 采用"直写优先、缓冲兜底"（`TcpConnection.cpp:81`）：
- output buffer 为空 → 直接 `write(fd)`。一次写完则零额外开销；写不完才把剩余部分入 output buffer 并关注 `EPOLLOUT`。
- 相比"一律入缓冲 + enableWriting"，小数据包能省一次 epoll_wait 往返，显著降低延迟。

### 5. 定时器系统：timerfd + 红黑树

从零实现 `TimerQueue`（仿 muduo），核心思想是把定时器**变成一个 fd**：

- `timerfd_create(CLOCK_MONOTONIC)` → 注册为 Channel 加入 epoll，定时器到期时 epoll 像处理普通 IO 一样返回可读。
- 所有定时器存 `std::set`（红黑树，`Entry = {到期时间, Timer*}`），O(logN) 增删查。
- 每次只把"最早到期"的定时器时间通过 `timerfd_settime(TFD_TIMER_ABSTIME)` 写入内核，到期后取走所有已到期项执行回调，repeat 定时器 `restart()` 重排。

**优点**：与网络 IO 统一在一个事件循环、无独立定时线程、无锁。EventLoop 暴露 `runAt / runAfter / runEvery` 三个接口。

### 6. 连接生命周期与延迟关闭

- `TcpConnection` 继承 `enable_shared_from_this`，跨线程回调（如 `send` 内部）捕获 `shared_from_this()`，保证对象在回调执行时必然存活。
- `Server::connections_` 用 `std::map<fd, shared_ptr<TcpConnection>>` + mutex 管理生命周期，`getAllConnections()` 返回 **shared_ptr 快照**，广播遍历期间连接并发关闭也不会悬垂。
- `forceClose()`（踢人）用**延迟关闭**状态机（`TcpConnection.cpp`）：第一次调用只把状态置 `kDisconnecting` 并把自己重新排入 `pendingFunctors`；当事件回调栈已退出、第二次进入时才真正 `handleClose`。**避免在用户回调栈内析构对象导致悬垂。**

### 7. 心跳超时回收半开连接

客户端拔网线/进程崩溃后，TCP 无法感知对端消失，连接永远占着资源。基于定时器实现心跳：

- `Server::setHeartbeat(timeout, checkInterval)`：在 main reactor 上 `runEvery` 一个扫描定时器。
- 每次扫描遍历连接表，`conn->isIdle(timeout)` 判定连接空闲（距上次收到数据）超过阈值 → `forceClose()` 强制关闭。
- 被踢连接正常走 `handleClose → closeCallback`，应用层照常收到"xx left"通知。

## 代码结构

```
reactor/
├── EventLoop.h/.cpp        # 事件循环核心：epoll 封装、Channel 管理、跨线程调度、定时器接口
├── Channel.h/.cpp          # fd 与事件的封装：注册/分发到具体回调
├── Timer.h/.cpp            # 单个定时器（到期时间 + 回调 + 重复间隔）
├── TimerQueue.h/.cpp       # 定时器队列：timerfd + 红黑树
├── Acceptor.h/.cpp         # 监听 fd：accept4 + 批量 accept + 新连接回调
├── TcpConnection.h/.cpp    # 单个连接：读/写/关/错处理、send/shutdown/forceClose、状态机
├── Server.h/.cpp           # 应用层服务器：连接表、回调注册、连接分发、心跳扫描
├── Buffer.h/.cpp           # 应用层缓冲区：readv 分散读、指针式读写
├── EventLoopThread.h/.cpp  # 线程 + 专属 EventLoop
├── EventLoopThreadPool.h/.cpp  # Sub Reactor 线程池 + round-robin 分发
├── server.cpp              # 服务器入口 / 聊天室 demo（功能验证）
└── client.cpp              # 命令行客户端
```

## 构建与运行

```bash
cd reactor/build
cmake ..
make

# 服务器：默认端口 8080，2 个 Sub Reactor 线程
./re_server
# 指定端口 + IO 线程数（0 = 单线程模式）
./re_server 9090 4

# 客户端（另开终端）
./re_client
```

## 功能验证（聊天室 demo）

基于该库实现聊天室作为**验证手段**，实测以下行为全部正常：

- 多客户端连接被 round-robin 均匀分发到不同 Sub Reactor 线程，并行处理；
- 任意用户发言实时广播给所有在线用户（含自己）；
- 上线/下线广播 `joined / left` 通知；
- 心跳：空闲超时连接被强制断开并广播离线（模拟拔网线场景）。

验证脚本：

```bash
python3 test_chat.py   # 多客户端模拟：上线/发言/离线广播 + 心跳踢线
```

## 演进史（从零实现的过程）

每个阶段都独立可运行，展示了网络模型的学习路径：

| 目录 | 阶段 | 解决的问题 |
|---|---|---|
| `select/` | 入门 | select 多路复用，但存在 1024 fd 上限、O(n) 扫描、跨平台差异 |
| `epoll/` | 进阶 | 换用 epoll，事件驱动、O(1) 就绪通知，但仍单线程串行 |
| `oneThread2oneFd/` | 多线程模型 | 每连接一个线程，但线程开销大、无上界、C10K 困难 |
| `reactor/` | 终版 | 主从 Reactor 事件驱动 + 非阻塞 IO + 线程池，兼顾并发与可控 |
| `benchmark/` | 压测 | 高性能压测客户端，用于对比不同模型的吞吐 |
| `os_practice/` | 前置 | fork / exec / pipe 等操作系统基础练习 |

## 局限与后续方向

- **单 Acceptor，未处理惊群**：多 Reactor 下只有一个主线程 accept，天然无惊群；若追求多核 accept，可扩展 `SO_REUSEPORT` 或 `EPOLLEXCLUSIVE`。
- **无 TCP 背压**：output buffer 无上限，未实现 muduo 的 `setHighWaterMarkCallback` 保护慢客户端。
- **未实现 `cancel()`**：定时器暂不支持取消，已留出扩展设计。
- **性能数据待补**：
