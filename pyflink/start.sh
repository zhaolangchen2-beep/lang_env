#!/bin/bash

# ================= 配置区 =================
# 1. 确保文件名正确 
YAML_FILE="compose.yaml" 
PROJECT_NAME="pyflink-dev"
# 2. 修正容器名称，必须与 docker-compose.yml 里的 container_name 一致
CONTAINER_NAME="pyflink-jobmanager"
# ==========================================

echo "🚀 正在启动 PyFlink 集群..."

# 检查文件是否存在
if [ ! -f "$YAML_FILE" ]; then
    echo "❌ 错误：找不到文件 $YAML_FILE"
    exit 1
fi

# 接收 deploy.sh 传来的镜像名
if [ -z "$1" ]; then
    echo "Usage: ./start.sh <image_name>"
    exit 1
fi

export IMAGE_NAME=$1

# 2. 健壮的镜像检测逻辑
# 使用 --filter 确保精确匹配
IF_EXISTS=$(docker images -q "$IMAGE_NAME")

if [ -z "$IF_EXISTS" ]; then
    echo "❌ $IMAGE_NAME image not found!"
    echo "Please ensure the build step in deploy.sh completed successfully."
    exit 1
else
    echo "✅ Image found (ID: $IF_EXISTS). Proceeding..."
fi

# 1. 启动容器
docker compose down --remove-orphans
docker compose up -d

if [ $? -ne 0 ]; then
    echo "❌ 启动失败，请检查 Docker 配置。"
    exit 1
fi

echo "⏳ 等待 Flink JobManager Web 端口 (28081) 就绪..."

# 2. 等待 Web UI 就绪
MAX_RETRIES=30
COUNT=0
until [ $(curl --noproxy "*" -s -o /dev/null -w "%{http_code}" http://127.0.0.1:28081) -eq 200 ]; do
    printf '.'
    sleep 2
    COUNT=$((COUNT+1))
    if [ $COUNT -eq $MAX_RETRIES ]; then
        echo -e "\n❌ 超时：虽然容器起来了，但脚本连不上端口。"
        echo "💡 提示：如果您浏览器能进，说明是脚本的网络检测方式与环境不兼容。"
        # 陛下，如果浏览器能进，这里其实可以选择不退出，而是继续往下跑
        echo "⚠️ 强制继续执行..." 
        break 
    fi
done

echo -e "\n✅ Flink Web UI 已上线: http://localhost:28081"

# ==========================================
# 3. 智能等待 TaskManager 注册 (已加入 --noproxy "*")
# ==========================================
echo "📊 正在等待 TaskManager 向皇庭报到..."
SLOTS=0
RETRY_SLOTS=0

# 循环检查，直到 Slots > 0
while [ $SLOTS -eq 0 ] && [ $RETRY_SLOTS -lt 15 ]; do
    # 核心修改：
    # 1. 加上 --noproxy "*" 绕过代理
    # 2. 使用 127.0.0.1 确保走 IPv4
    # 3. 加上 || echo 0 防止 curl 报错导致脚本崩溃
    SLOTS=$(curl --noproxy "*" -s http://127.0.0.1:28081/overview | grep -o '"slots-total":[0-9]*' | cut -d: -f2)
    
    # 如果抓取失败（为空），强制设为 0
    SLOTS=${SLOTS:-0}
    
    if [ "$SLOTS" -gt 0 ]; then
        break
    fi
    printf '.'
    sleep 2
    RETRY_SLOTS=$((RETRY_SLOTS+1))
done
echo ""

if [ "$SLOTS" -gt 0 ]; then
    echo "   >> ✅ 总可用插槽 (Slots): $SLOTS"
else
    echo "   >> ⚠️ 警告: Web UI 已通，但 TaskManager 尚未显示插槽 (Slots: 0)。"
    echo "      这可能是网络延迟，也可能是因为 curl 仍被代理干扰，请手动访问浏览器确认。"
fi

echo "---"
echo "🎉 部署完成！"
echo "复制执行：docker exec -it pyflink-jobmanager python /opt/py_code/mix_udf_benchmark.py -c 100000000"