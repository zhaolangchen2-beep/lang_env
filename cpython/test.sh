#!/bin/bash
# ================================================================
# test.sh — CinderX 自动化压测与指标收集脚本
#
# 功能：
#   1. 调用容器内的 cinderx_test.sh 切换指定的 commit
#   2. 自动化执行编译、安装并运行 pyperformance
#   3. 收集并格式化打印最终的测试结果
# ================================================================

set -euo pipefail

# ────────────────────────────────────────────────────────────────
# 默认配置
# ────────────────────────────────────────────────────────────────
CONTAINER="cpython"
CINDERX_SCRIPT="/home/share/cinderx_test.sh"  # 容器内脚本路径
COMMIT="SKIP"
BENCHMARKS=""
CPU_AFFINITY=305
WARMUP=3
SKIP_BUILD=0
VERBOSE=false

# ────────────────────────────────────────────────────────────────
# 参数解析
# ────────────────────────────────────────────────────────────────
usage() {
    cat << EOF
Usage: ./test.sh [OPTIONS]

Options:
  -c, --commit <hash>      CinderX commit (default: SKIP, use local code)
  -b, --benchmarks <list>  Comma-separated benchmarks (e.g. nbody,json)
  -a, --affinity <core>    CPU affinity (default: $CPU_AFFINITY)
  -w, --warmup <N>         Warmup iterations (default: $WARMUP)
  -s, --skip-build         Skip cinderx compilation in container
  -v, --verbose            Show full execution logs
  -h, --help               Show this help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--commit)     COMMIT="$2";         shift 2 ;;
        -b|--benchmarks) BENCHMARKS="$2";     shift 2 ;;
        -a|--affinity)   CPU_AFFINITY="$2";   shift 2 ;;
        -w|--warmup)     WARMUP="$2";         shift 2 ;;
        -s|--skip-build) SKIP_BUILD=1;        shift ;;
        -v|--verbose)    VERBOSE=true;        shift ;;
        -h|--help)       usage ;;
        *) echo "❌ Unknown option: $1"; exit 1 ;;
    esac
done

# ────────────────────────────────────────────────────────────────
# 构建容器内执行命令
# ────────────────────────────────────────────────────────────────
# 将 test.sh 的参数映射到 cinderx_test.sh 的参数
INTERNAL_CMD="bash $CINDERX_SCRIPT"
[ "$COMMIT" != "SKIP" ] && INTERNAL_CMD+=" -c $COMMIT"
[ -n "$BENCHMARKS" ] && INTERNAL_CMD+=" -b \"$BENCHMARKS\""
INTERNAL_CMD+=" -a $CPU_AFFINITY -w $WARMUP"
[ "$SKIP_BUILD" -eq 1 ] && INTERNAL_CMD+=" -s"

# ────────────────────────────────────────────────────────────────
# 执行与数据收集
# ────────────────────────────────────────────────────────────────
log_file=$(mktemp /tmp/cinderx_run_XXXXXX.log)

echo "================================================================"
echo "  🚀 Starting CinderX Benchmark in Container: $CONTAINER"
echo "  Commit: $COMMIT | Benchmarks: ${BENCHMARKS:-all}"
echo "================================================================"

# 执行容器内脚本
set +e
if [ "$VERBOSE" = true ]; then
    docker exec -it "$CONTAINER" bash -lc "$INTERNAL_CMD" | tee "$log_file"
    EXIT_CODE=${PIPESTATUS[0]}
else
    echo "  [Running...] Internal script is executing. Please wait..."
    docker exec -i "$CONTAINER" bash -lc "$INTERNAL_CMD" > "$log_file" 2>&1
    EXIT_CODE=$?
fi
set -e

# ────────────────────────────────────────────────────────────────
# 结果解析与打印
# ────────────────────────────────────────────────────────────────
echo ""
if [ $EXIT_CODE -ne 0 ]; then
    echo "❌ Execution Failed (Exit Code: $EXIT_CODE)"
    echo "--- Last 10 lines of log ---"
    tail -n 10 "$log_file" | sed 's/^/  /'
    exit $EXIT_CODE
else
    echo "✅ Execution Completed Successfully."
    echo "----------------------------------------------------------------"
    echo "  Summary Results (Extracted from pyperformance):"
    echo "----------------------------------------------------------------"
    
    # 从日志中提取 pyperformance show 的摘要数据
    # 假设摘要以 "Performance version" 或 "Report file" 开始
    # 我们抓取最后 20 行包含时间数据的行
    grep -A 20 "Step 4/4" "$log_file" | sed 's/^/  /' || true
fi

echo "----------------------------------------------------------------"
echo "  Full log saved to: $log_file"
echo "================================================================"

rm -f "$log_file"