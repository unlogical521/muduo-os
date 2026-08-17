#!/usr/bin/env python3
"""
复现 re_server 关闭期崩溃(SIGABRT)。

机制假设:Server 析构时,connections_ 先于 threadPool_ 被销毁
→ 还开着的 TcpConnection 在 main 线程析构,同时子 reactor 线程仍在
   epoll_wait/dispatch → 对 channels_ std::map 与 epfd 的数据竞争。

触发条件:服务器关闭时仍有连接处于 kConnected(未 handleClose)状态。

用法:
  python3 repro_shutdown.py [循环次数] [连接数] [端口]
  默认: 循环 10 次,每次 500 连接,端口 8096
"""
import socket
import subprocess
import sys
import time
import signal
import os
import threading

BUILD = "/home/illogical/project/muduo/reactor/build"
N_LOOPS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
N_CONN = int(sys.argv[2]) if len(sys.argv) > 2 else 500
PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 8096
DEVNULL = open(os.devnull, "w")


def run_once(i):
    srv = subprocess.Popen(["./re_server", str(PORT), "2"],
                           cwd=BUILD, stdout=DEVNULL, stderr=DEVNULL)
    time.sleep(0.8)
    conns = []
    try:
        for _ in range(N_CONN):
            c = socket.create_connection(("127.0.0.1", PORT), timeout=2)
            c.setblocking(False)
            conns.append(c)
    except OSError as e:
        print(f"  [loop {i}] 建立连接失败: {e}")

    # 数据洪泛:每个连接持续发消息,让子 reactor 线程一直处于活跃派发状态
    stop = threading.Event()
    payload = b"x" * 64

    def flood(conn):
        while not stop.is_set():
            try:
                conn.sendall(payload)
            except OSError:
                break
            time.sleep(0.002)

    threads = [threading.Thread(target=flood, args=(c,), daemon=True)
               for c in conns]
    for t in threads:
        t.start()

    time.sleep(1.0)  # 洪泛进行中,SIGTERM

    # 关键:洪泛活跃时 SIGTERM
    srv.send_signal(signal.SIGTERM)
    try:
        rc = srv.wait(timeout=8)
    except subprocess.TimeoutExpired:
        srv.kill()
        rc = "TIMEOUT"
    stop.set()
    for c in conns:
        try:
            c.close()
        except OSError:
            pass
    return rc


def main():
    crashes = 0
    for i in range(N_LOOPS):
        rc = run_once(i)
        status = "CRASH" if (isinstance(rc, int) and rc != 0) else "clean"
        if status == "CRASH":
            crashes += 1
        print(f"  loop {i:2d}: exit={rc}  -> {status}")
    print(f"\n=== 崩溃 {crashes}/{N_LOOPS} ===")
    return 1 if crashes else 0


if __name__ == "__main__":
    sys.exit(main())
