#!/bin/bash
set -e

# 参数解析
REDEPLOY=false
MODULE_NAME=""
PYTHON_VERSION="3.11.0"
OUT_DIR="$(pwd)/test_results"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -m|--module) MODULE_NAME="$2"; shift ;;
        -v|--version) PYTHON_VERSION="$2"; shift ;;
        --redeploy) REDEPLOY=true ;;
        --out-dir) OUT_DIR="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ -z "$MODULE_NAME" ]; then
    echo "Error: Module name is required (-m)."
    exit 1
fi

# 创建结果转储目录
CURRENT_RESULT_DIR="$OUT_DIR/${MODULE_NAME}_${PYTHON_VERSION}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$CURRENT_RESULT_DIR"

echo "========== Starting Test Session =========="
echo "Module: $MODULE_NAME"
echo "Python: $PYTHON_VERSION"
echo "Results: $CURRENT_RESULT_DIR"

# 1. 部署阶段 (如果指定了 --redeploy)
if [ "$REDEPLOY" = true ]; then
    echo "Requesting Redeployment..."
    ./deploy.sh "$MODULE_NAME" "$PYTHON_VERSION"
else
    echo "Skipping deployment (assuming container is running)."
fi

# 2. 测试执行阶段
MODULE_TEST_SCRIPT="./$MODULE_NAME/test.sh"

if [ ! -f "$MODULE_TEST_SCRIPT" ]; then
    echo "Error: Submodule test script not found: $MODULE_TEST_SCRIPT"
    exit 1
fi

echo "Executing Submodule Test..."
chmod +x "$MODULE_TEST_SCRIPT"

# 调用子模块测试脚本
# 传入核心参数：输出目录
# 子模块应该把结果写到 $1 指定的目录里
"$MODULE_TEST_SCRIPT" \
    --out-dir "$CURRENT_RESULT_DIR" \
    --container-name "${MODULE_NAME}_container" # 假设这是 start.sh 命名的容器名

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "========== Test PASSED =========="
else
    echo "========== Test FAILED =========="
fi

echo "Logs available at: $CURRENT_RESULT_DIR"
exit $TEST_EXIT_CODE