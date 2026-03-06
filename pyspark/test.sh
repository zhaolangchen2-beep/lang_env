#!/bin/bash
# ================================================================
# test.sh — PySpark UDF Benchmark 测试脚本
#
# 功能：
#   1. 在容器内执行 spark-submit 运行基准测试
#   2. 从输出中提取 Benchmark 指标和端到端时长
#   3. 支持单个 query 或 --all 模式跑全部
#
# 用法：
#   ./test.sh -q tpch_q1                           # 单个 query（静默）
#   ./test.sh -q tpch_q1 -v                        # 显示完整 spark 输出
#   ./test.sh -q all -c 10000000                   # 跑全部
# ================================================================

set -euo pipefail

# ────────────────────────────────────────────────────────────────
# 默认配置
# ────────────────────────────────────────────────────────────────
CONTAINER="spark-master"
MASTER="local[*]"
JAR="/opt/spark/apps/spark-benchmark-udfs-java.jar"
RUN_PY="/opt/spark/apps/run.py"
ROW_COUNT=10000000
PARALLELISM=1
QUERY=""
TASKSET_CORES=""
EXTRA_SPARK_ARGS=""
EXTRA_RUN_ARGS=""
VERBOSE=false

ALL_QUERIES=(
    add cdf
    tpch_q1 tpch_q2 tpch_q3 tpch_q4 tpch_q5 tpch_q6
    tpch_q7 tpch_q8 tpch_q9 tpch_q10 tpch_q11 tpch_q12
    tpch_q13 tpch_q14 tpch_q15 tpch_q16 tpch_q17 tpch_q18
    tpch_q19 tpch_q20 tpch_q21 tpch_q22
)

# ────────────────────────────────────────────────────────────────
# 参数解析
# ────────────────────────────────────────────────────────────────
usage() {
    cat << 'EOF'
Usage: ./test.sh [OPTIONS]

Required:
  -q, --query <name|all>     Query to run (e.g. tpch_q1, add, all)

Benchmark options:
  -c, --row-count <N>        Number of input rows (default: 10000000)
  -a, --parallelism <N>      Spark parallelism (default: 1)

Spark options:
  --master <url>             Spark master (default: local[*])
  --jars <path>              JAR path in container
  --spark-args <args>        Extra spark-submit arguments (quoted)

Execution options:
  --container <name>         Docker container name (default: spark-master)
  --taskset <cores>          CPU cores to pin (e.g. "0-7")
  --run-args <args>          Extra run.py arguments (quoted)

Output options:
  -v, --verbose              Show full spark-submit output
  -h, --help                 Show this help

Examples:
  ./test.sh -q tpch_q1
  ./test.sh -q all -c 20000000 -v
  ./test.sh -q tpch_q1 --taskset "0-7" --master "local[4]"
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -q|--query)        QUERY="$2";            shift 2 ;;
        -c|--row-count)    ROW_COUNT="$2";         shift 2 ;;
        -a|--parallelism)  PARALLELISM="$2";       shift 2 ;;
        --master)          MASTER="$2";            shift 2 ;;
        --jars)            JAR="$2";               shift 2 ;;
        --container)       CONTAINER="$2";         shift 2 ;;
        --taskset)         TASKSET_CORES="$2";     shift 2 ;;
        --spark-args)      EXTRA_SPARK_ARGS="$2";  shift 2 ;;
        --run-args)        EXTRA_RUN_ARGS="$2";    shift 2 ;;
        -v|--verbose)      VERBOSE=true;           shift ;;
        -h|--help)         usage ;;
        *)
            echo "❌ Unknown option: $1"
            echo "   Run './test.sh --help' for usage."
            exit 1
            ;;
    esac
done

if [ -z "$QUERY" ]; then
    echo "❌ Missing required argument: -q <query|all>"
    echo "   Run './test.sh --help' for usage."
    exit 1
fi

# ────────────────────────────────────────────────────────────────
# 构建 query 列表
# ────────────────────────────────────────────────────────────────
if [ "$QUERY" = "all" ]; then
    QUERIES=("${ALL_QUERIES[@]}")
else
    QUERIES=("$QUERY")
fi

# ────────────────────────────────────────────────────────────────
# 构建执行命令
# ────────────────────────────────────────────────────────────────
build_cmd() {
    local q="$1"
    local cmd=""

    if [ -n "$TASKSET_CORES" ]; then
        cmd="taskset -c $TASKSET_CORES "
    fi

    cmd+="spark-submit"
    cmd+=" --master $MASTER"
    cmd+=" --jars $JAR"

    if [ -n "$EXTRA_SPARK_ARGS" ]; then
        cmd+=" $EXTRA_SPARK_ARGS"
    fi

    cmd+=" $RUN_PY"
    cmd+=" -q $q"
    cmd+=" -c $ROW_COUNT"
    cmd+=" -a $PARALLELISM"

    if [ -n "$EXTRA_RUN_ARGS" ]; then
        cmd+=" $EXTRA_RUN_ARGS"
    fi

    echo "$cmd"
}

# ────────────────────────────────────────────────────────────────
# 提取指标
# ────────────────────────────────────────────────────────────────
extract_metrics() {
    local output="$1"
    local query_name="$2"

    # 提取 Benchmark 行（排除 WARMUP）
    local bench_lines
    bench_lines=$(echo "$output" | grep '\[.*Benchmark\]' | grep -v 'WARMUP' || true)

    if [ -z "$bench_lines" ]; then
        printf "  %-12s | ⚠️  No benchmark data\n" "$query_name"
        return
    fi

    # 取倒数第二行（最后一批可能不满，不准）
    local total_lines
    total_lines=$(echo "$bench_lines" | wc -l)

    local target_line
    if [ "$total_lines" -ge 2 ]; then
        target_line=$(echo "$bench_lines" | tail -2 | head -1)
    else
        target_line=$(echo "$bench_lines" | tail -1)
    fi

    # 提取字段
    local avg_udf avg_overhead throughput wallclock
    avg_udf=$(echo "$target_line" | grep -oP 'Avg UDF=\K[0-9]+' || echo "N/A")
    avg_overhead=$(echo "$target_line" | grep -oP 'Avg Overhead=\K[0-9]+' || echo "N/A")
    throughput=$(echo "$target_line" | grep -oP 'Throughput=\K[0-9]+' || echo "N/A")
    wallclock=$(echo "$target_line" | grep -oP 'WallClock=\K[0-9.]+' || echo "N/A")

    # 提取端到端时长
    local duration
    duration=$(echo "$output" | grep -oP 'Duration=\K[0-9.]+' | tail -1 || true)
    if [ -z "$duration" ]; then
        duration=$(echo "$output" | grep -oP 'Wall time\s*:\s*\K[0-9.]+' || echo "N/A")
    fi

    printf "  %-12s | UDF=%4s ns | Overhead=%5s ns | Throughput=%7s rows/s | Duration=%6s s\n" \
        "$query_name" \
        "$avg_udf" \
        "$avg_overhead" \
        "$throughput" \
        "$duration"
}

# ────────────────────────────────────────────────────────────────
# 打印配置头
# ────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  PySpark UDF Benchmark"
echo "================================================================"
echo "  Rows=$ROW_COUNT  Parallelism=$PARALLELISM  Master=$MASTER"
[ -n "$TASKSET_CORES" ] && echo "  Taskset=cores $TASKSET_CORES"
echo "----------------------------------------------------------------"

# ────────────────────────────────────────────────────────────────
# 表头
# ────────────────────────────────────────────────────────────────
echo ""
printf "  %-12s | %-10s | %-14s | %-18s | %s\n" \
    "Query" "UDF" "Overhead" "Throughput" "Duration"
echo "  ------------ | ---------- | -------------- | ------------------ | ----------"

# ────────────────────────────────────────────────────────────────
# 执行测试
# ────────────────────────────────────────────────────────────────
for q in "${QUERIES[@]}"; do
    cmd=$(build_cmd "$q")
    tmpfile=$(mktemp /tmp/bench_${q}_XXXXXX.log)

    if [ "$VERBOSE" = true ]; then
        echo ""
        echo "── Running: $q ──"
        echo "   CMD: docker exec -it $CONTAINER bash -c \"$cmd\""
        echo ""
    fi

    # 执行命令
    set +e
    if [ "$VERBOSE" = true ]; then
        # verbose: 同时输出到终端和文件
        docker exec -it "$CONTAINER" bash -c "$cmd" 2>&1 | tee "$tmpfile"
    else
        # 静默: 只写文件，不输出到终端
        docker exec -it "$CONTAINER" bash -c "$cmd" > "$tmpfile" 2>&1
    fi
    exit_code=${PIPESTATUS[0]}
    set -e

    if [ "$VERBOSE" = true ]; then
        echo ""
    fi

    if [ $exit_code -ne 0 ]; then
        printf "  %-12s | ❌ FAILED (exit code %d)\n" "$q" "$exit_code"
        if [ "$VERBOSE" = false ]; then
            echo "             Run with -v to see full output"
        fi
    else
        extract_metrics "$(cat "$tmpfile")" "$q"
    fi

    rm -f "$tmpfile"
done

# ────────────────────────────────────────────────────────────────
# 尾部
# ────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  Config: -c $ROW_COUNT -a $PARALLELISM --master $MASTER"
[ -n "$TASKSET_CORES" ] && echo "  Taskset: cores $TASKSET_CORES"
echo "================================================================"