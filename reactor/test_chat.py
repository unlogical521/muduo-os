#!/usr/bin/env python3
"""
reactor 网络库功能验证脚本（配合 server.cpp 聊天室 demo）

覆盖场景：
  1. 上线广播  —— 新连接上线，其它连接收到 "[chat] xx joined"
  2. 发言广播  —— 任意用户发言，所有在线连接（含自己）收到 "[chat] xx msg"
  3. 离线广播  —— 连接断开，其它连接收到 "[chat] xx left"
  4. 心跳踢线  —— 空闲超时连接被服务器强制断开（并广播 left）
  5. 多线程分发 —— 多连接被 round-robin 分发到不同 Sub Reactor 线程（看服务器日志）

用法：
  先启动服务器（开一个终端）：
    cd reactor/build && ./re_server 8080 2
  再运行本脚本（另一个终端）：
    python3 test_chat.py
  脚本退出码 0 = 全部通过；非 0 = 有断言失败。

依赖：仅 Python 标准库。
"""
import socket
import sys
import threading
import time

HOST = "127.0.0.1"
PORT = 8080


class TestClient:
    """模拟一个聊天室客户端：后台线程收消息，等待服务器关闭时置 closed 事件。"""

    def __init__(self, name):
        self.name = name
        self.received = []            # 已解析出的完整行（去掉 \r\n）
        self.closed = threading.Event()  # 服务器关闭连接时置位
        self.sock = socket.create_connection((HOST, PORT), timeout=2)
        self.sock.settimeout(0.3)
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        buf = b""
        while not self.closed.is_set():
            try:
                data = self.sock.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:                      # 服务器关闭（心跳踢线 / 正常关闭）
                self.received.append("<server closed>")
                self.closed.set()
                break
            buf += data
            while b"\r\n" in buf:             # 按聊天室协议 \r\n 分行
                line, buf = buf.split(b"\r\n", 1)
                self.received.append(line.decode())

    def send_line(self, msg):
        self.sock.sendall((msg + "\r\n").encode())

    def wait(self, seconds):
        time.sleep(seconds)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def check(cond, label, detail=""):
    """断言 + 打印，统计通过/失败数。"""
    global passed, failures
    if cond:
        print(f"  [PASS] {label}")
        passed += 1
    else:
        print(f"  [FAIL] {label}  {detail}")
        failures += 1


def main():
    global passed, failures
    passed = 0
    failures = 0

    print("== 场景 1/2/3：上线/发言/离线广播 ==")
    a = TestClient("A")
    a.wait(0.4)  # 给服务器一点时间完成注册
    assert a.received, "连接 A 未收到欢迎语，服务器可能未启动？"
    print(f"  A 上线收到: {a.received[0]}")

    b = TestClient("B")
    b.wait(0.4)
    check("[chat] conn-2 joined" in a.received, "A 收到 B 上线广播")

    a.send_line("hello")
    a.wait(0.4)
    check("[chat] conn-1 hello" in a.received, "A 收到自己的发言广播")
    check("[chat] conn-1 hello" in b.received, "B 收到 A 的发言广播")

    b.send_line("hi")
    b.wait(0.4)
    check("[chat] conn-2 hi" in a.received, "A 收到 B 的发言广播")

    b.close()  # 正常下线
    a.wait(0.4)
    check("[chat] conn-2 left" in a.received, "A 收到 B 离线广播")

    print("== 场景 4：心跳踢线（空闲超时被强制断开）==")
    idle = TestClient("idle")     # 连上后保持沉默
    active = TestClient("active") # 持续发言，应保持存活
    active.wait(0.4)

    # active 每 2s 发一条消息，持续 8s；idle 全程沉默
    for i in range(4):
        active.send_line(f"ping {i}")
        active.wait(2)
        if idle.closed.is_set():
            break

    check(idle.closed.is_set(), "沉默连接被心跳超时断开")
    check(not active.closed.is_set(), "持续发言的连接未被断开")
    check("[chat] conn-3 left" in active.received, "活跃连接收到沉默连接被踢的广播")

    idle.close()
    active.close()
    a.close()

    print("== 场景 5：多线程分发（需查看服务器日志）==")
    print("  启动服务器后日志中会出现多个不同的 'reactor thread' 线程 id,")
    print("  说明连接被 round-robin 分发到不同 Sub Reactor 线程并行处理。")

    print()
    print(f"=== 断言结果：{passed} 通过 / {failures} 失败 ===")
    if failures == 0:
        print("=== 全部通过 ✔ ===")
        return 0
    print(f"=== {failures} 项断言失败 ===")
    return 1


if __name__ == "__main__":
    sys.exit(main())
