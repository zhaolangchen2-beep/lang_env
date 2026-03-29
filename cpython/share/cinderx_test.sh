#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 配置区
# ============================================================
CINDERX_SRC="/opt/cinderx"
WORK_DIR="/home/share"
RESULT_DIR="/home/share/results"
PYTHON="python3"
PIP="pip"

# JIT 配置
JIT_LIST_FILE="jit_list.txt"
CPU_AFFINITY=305
WARMUP=3

# ============================================================
# 用法
# ============================================================
usage() {
    cat <<EOF
用法:
  $0 -c <commit> [-b <benchmarks>] [-a <affinity>] [-w <warmup>]

参数:
  -c <commit>       cinderx commit (hash / tag / branch)
  -b <benchmarks>   逗号分隔的用例，如 "nbody,fannkuch,chaos"（默认全部）
  -a <affinity>     CPU 亲和性（默认 ${CPU_AFFINITY}）
  -w <warmup>       warmup 次数（默认 ${WARMUP}）
  -s <skip_build>
示例:
  $0 -c a1b2c3d4 -b nbody
  $0 -c a1b2c3d4 -b "nbody,fannkuch" -a 42 -w 3
EOF
    exit 1
}

# ============================================================
# 解析参数
# ============================================================
COMMIT="SKIP"
BENCHMARKS=""
SKIP_BUILD=0

while getopts "c:b:a:w:sh" opt; do
    case ${opt} in
        c) COMMIT="${OPTARG}" ;;
        b) BENCHMARKS="${OPTARG}" ;;
        a) CPU_AFFINITY="${OPTARG}" ;;
        w) WARMUP="${OPTARG}" ;;
		s) SKIP_BUILD=1 ;;
        h) usage ;;
        *) usage ;;
    esac
done

# ============================================================
# 工具函数
# ============================================================
log() { echo -e "\n\033[1;36m▶ $1\033[0m"; }
fail() { echo -e "\n\033[1;31m❌ $1\033[0m" >&2; exit 1; }
elapsed() { printf "%02d:%02d:%02d" $(($1/3600)) $(($1%3600/60)) $(($1%60)); }

# ============================================================
# Step 0: 准备环境
# ============================================================
START_TIME=$(date +%s)
mkdir -p "${RESULT_DIR}"


# ============================================================
# Step 1: checkout 指定 commit
# ============================================================
log "Step 1/4: checkout ${COMMIT}"
cd "${CINDERX_SRC}"
if [[ "${COMMIT}" == "SKIP" ]]; then
	echo "未指定 commit，将直接基于当前本地代码进行编译"
    FULL_HASH=$(git rev-parse HEAD)
    SHORT_HASH="${FULL_HASH:0:8}-local" 
	COMMIT_MSG=$(git log -1 --pretty=format:"%s" | head -c 60)
else 
	log "正在 checkout 到 ${COMMIT}..."
	git fetch --all --quiet 2>/dev/null || true
	git switch --detach "${COMMIT}" || fail "无法 checkout: ${COMMIT}"
	FULL_HASH=$(git rev-parse HEAD)
	SHORT_HASH=${FULL_HASH:0:8}
	COMMIT_MSG=$(git log -1 --pretty=format:"%s" | head -c 60)
fi
echo "  commit : ${FULL_HASH}"
echo "  message: ${COMMIT_MSG}"

# ============================================================
# Step 2: 编译安装 cinderx
# ============================================================
log "Step 2/4: 编译安装 cinderx"

if [[ ${SKIP_BUILD} == 0 ]]; then
	${PIP} uninstall -y cinderx 2>/dev/null || true
	git clean -xdf
	${PIP} install . 2>&1 | tail -5 || fail "cinderx 编译安装失败"
fi
# 验证
${PYTHON} -c "import cinderx; print(f'  cinderx OK: {cinderx.__file__}')" \
    || fail "cinderx 导入失败"

# ============================================================
# Step 3: 跑 pyperformance
# ============================================================
log "Step 3/4: 运行 pyperformance"

TIMESTAMP=$(date +%m%d-%H%M%S)
OUTPUT_FILE="${RESULT_DIR}/cinderx-${SHORT_HASH}-${TIMESTAMP}.json"

cd "${WORK_DIR}"

# 构建 benchmark 参数
BENCH_ARGS=()
if [[ -n "${BENCHMARKS}" ]]; then
    # pyperformance 原生支持逗号分隔，直接传即可
    BENCH_ARGS+=("-b" "${BENCHMARKS}")
    echo "  用例: ${BENCHMARKS}"
else
    echo "  运行全部用例"
fi

# 设置环境变量并执行
PYTHONJITTYPEANNOTATIONGUARDS=1 \
PYTHONJITENABLEHIRINLINER=1 \
PYTHONJITENABLEJITLISTWILDCARDS=1 \
PYTHONJITAUTO=2 \
PYTHONJITSPECIALIZEDOPCODES=1 \
PYTHONJITLISTFILE="${JIT_LIST_FILE}" \
${PYTHON} -m pyperformance run \
    --affinity="${CPU_AFFINITY}" \
    --warmup "${WARMUP}" \
    --inherit-environ http_proxy,https_proxy,LD_LIBRARY_PATH,PYTHONJITAUTO,PYTHONJITSPECIALIZEDOPCODES,PYTHONJITLISTFILE,PYTHONJITENABLEJITLISTWILDCARDS,PYTHONJITTYPEANNOTATIONGUARDS,PYTHONJITENABLEHIRINLINER \
    -o "${OUTPUT_FILE}" \
    "${BENCH_ARGS[@]}" \
    || fail "pyperformance 运行失败"

# ============================================================
# Step 4: 完成
# ============================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log "Step 4/4: 完成 ✅"
echo "  commit  : ${SHORT_HASH} (${COMMIT_MSG})"
echo "  报告文件: ${OUTPUT_FILE}"
echo "  总耗时  : $(elapsed ${DURATION})"
echo ""

# 打印摘要
${PYTHON} -m pyperformance show "${OUTPUT_FILE}" 2>/dev/null | tail -20 || true
