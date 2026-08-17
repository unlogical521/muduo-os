#!/bin/bash
#
# 并发性能基准测试脚本
#
# 对每个 sub-reactor 线程数启动一次 bench_server，然后跑不同并发连接数的
# perf_client，解析 CSV 行，汇总成对比表。
#
# 用法： bash run_bench.sh [msg_size] [msgs_per_conn]
#   默认 msg_size=64, msgs_per_conn=200
#
set -u

MSGSIZE=${1:-64}
MSGCNT=${2:-200}
PORT=8080
BENCH_SERVER=/home/illogical/project/muduo/reactor/build/bench_server
PERF_CLIENT=/home/illogical/project/muduo/benchmark/perf_client
RESULT_DIR=/home/illogical/project/muduo/benchmark/results
mkdir -p "$RESULT_DIR"

THREADS_LIST="1 2 4 8"
CONN_LIST="1 10 50 100 200 500"

RESULTS_CSV="$RESULT_DIR/bench_${MSGSIZE}B.csv"
# 表头
echo "threads,conn,msgs,size,rps,mbps,avg_us,p50_us,p95_us,p99_us,count" > "$RESULTS_CSV"

echo "===== 并发性能基准测试 ====="
echo "消息大小: $MSGSIZE B, 每连接消息数: $MSGCNT"
echo "目标: 127.0.0.1:$PORT"
echo ""

for T in $THREADS_LIST; do
    # 启动服务器
    "$BENCH_SERVER" "$PORT" "$T" > "$RESULT_DIR/server_t${T}.log" 2>/dev/null &
    SRV_PID=$!
    # 等待 READY
    for i in $(seq 1 100); do
        if grep -q "BENCH_SERVER_READY" "$RESULT_DIR/server_t${T}.log" 2>/dev/null; then
            break
        fi
        sleep 0.05
    done
    grep -q "BENCH_SERVER_READY" "$RESULT_DIR/server_t${T}.log" 2>/dev/null \
        || { echo "[FAIL] server (ioThreads=$T) 未就绪"; continue; }
    echo "── ioThreads=$T（服务器已启动）──────────────────────────"

    for C in $CONN_LIST; do
        OUT=$("$PERF_CLIENT" "$C" "$MSGCNT" "$MSGSIZE" 2>/dev/null)
        # 取 CSV 行（以 server, 开头）
        CSV_LINE=$(echo "$OUT" | grep -E '^server,' | tail -1)
        if [ -z "$CSV_LINE" ]; then
            echo "  conn=$C : [FAIL] 无结果"
            continue
        fi
        # CSV_LINE: server,C,MSGCNT,MSGSIZE,elapsed,rps,mbps,avg,min,max,p50,p95,p99,count
        # 用 awk 抽字段
        read -r M C2 MSC SIZE EL RPS MBPS AVG MIN MAX P50 P95 P99 CNT <<< \
            $(echo "$CSV_LINE" | tr ',' ' ')
        echo "$T,$C,$MSC,$SIZE,$RPS,$MBPS,$AVG,$P50,$P95,$P99,$CNT" >> "$RESULTS_CSV"
        printf "  conn=%-4s  RPS=%8.0f  avg=%7.1fus  p50=%7.1f  p95=%7.1f  p99=%7.1f\n" \
            "$C" "$RPS" "$AVG" "$P50" "$P95" "$P99"
    done

    # 停服务器（必须等它真正退出，否则端口残留会导致下一轮 bind 失败 → LOG_FATAL → abort）
    kill -TERM "$SRV_PID" 2>/dev/null
    for i in $(seq 1 100); do
        kill -0 "$SRV_PID" 2>/dev/null || break
        sleep 0.05
    done
    # 兜底：超时仍不退则强杀（正常情况下 Server 析构的 closeNow+stop 会在毫秒级完成）
    kill -0 "$SRV_PID" 2>/dev/null && { echo "  [warn] server 未在 5s 内退出，强制 kill"; kill -9 "$SRV_PID"; }
    wait "$SRV_PID" 2>/dev/null
    sleep 0.2
    echo ""
done

echo "===== 结果已写入 $RESULTS_CSV ====="
