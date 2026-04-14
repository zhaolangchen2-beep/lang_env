#!/bin/bash

echo "==========================================="
echo "Starting Spark Cluster with Docker Compose"
echo "==========================================="

# 检查Docker Compose是否安装
if ! command -v docker compose &> /dev/null; then
    echo "❌ Error: docker-compose is not installed"
    echo "Please install docker-compose:"
    echo "  sudo apt install docker-compose  # Ubuntu/Debian"
    echo "  or download from: https://docs.docker.com/compose/install/"
    exit 1
fi

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Starting Docker..."
    sudo systemctl start docker 2>/dev/null || echo "Please start Docker manually"
    sleep 5
fi

# 参数解析
IMAGE_NAME=""
PROJECT_NAME="pyspark"
PORT_OFFSET=0

usage() {
    echo "Usage: ./start.sh <image_name> [--project-name <name>] [--port-offset <offset>]"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

IMAGE_NAME="$1"
shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-name)
            PROJECT_NAME="$2"
            shift 2
            ;;
        --port-offset)
            PORT_OFFSET="$2"
            shift 2
            ;;
        *)
            echo "❌ Unknown option: $1"
            usage
            ;;
    esac
done

if ! [[ "$PORT_OFFSET" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --port-offset must be a non-negative integer."
    exit 1
fi

if [ ! -f .env ]; then
    echo "❌ Error: Missing .env file in $(pwd)"
    echo "   Please copy .env.example to .env before starting the cluster."
    echo "   Example: cp .env.example .env"
    exit 1
fi

export IMAGE_NAME
export PROJECT_NAME
export CONTAINER_PREFIX="$PROJECT_NAME"
export HOST_PORT_MASTER_UI=$((8080 + PORT_OFFSET))
export HOST_PORT_WORKER_1_UI=$((8081 + PORT_OFFSET))
export HOST_PORT_WORKER_2_UI=$((8082 + PORT_OFFSET))
export HOST_PORT_MASTER_RPC=$((7077 + PORT_OFFSET))
export HOST_PORT_MASTER_APP_UI=$((4040 + PORT_OFFSET))
export HOST_PORT_HISTORY_UI=$((18080 + PORT_OFFSET))
export HOST_PORT_JUPYTER=$((8888 + PORT_OFFSET))

# 设置权限
chmod +x configs/spark-env.sh 2>/dev/null || true

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

# 启动集群
echo "🚀 Starting Spark cluster..."
echo "   Project name : $PROJECT_NAME"
echo "   Port offset  : $PORT_OFFSET"
docker compose -p "$PROJECT_NAME" --env-file .env down --remove-orphans
docker compose -p "$PROJECT_NAME" --env-file .env up -d

echo "⏳ Waiting for services to start (40 seconds)..."
sleep 40

# 显示状态
echo "📊 Cluster status:"
docker compose -p "$PROJECT_NAME" --env-file .env ps

# 显示访问信息
echo ""
echo "==========================================="
echo "✅ Spark Cluster Started Successfully!"
echo "==========================================="
echo ""
echo "🌐 Web Interfaces:"
echo "   Spark Master UI:     http://localhost:${HOST_PORT_MASTER_UI}"
echo "   Spark Worker 1 UI:   http://localhost:${HOST_PORT_WORKER_1_UI}"
echo "   Spark Worker 2 UI:   http://localhost:${HOST_PORT_WORKER_2_UI}"
echo "   Spark History UI:    http://localhost:${HOST_PORT_HISTORY_UI}"
echo "   Jupyter Notebook:    http://localhost:${HOST_PORT_JUPYTER}"
echo ""
echo "🔗 Connection URLs:"
echo "   Master URL:          spark://localhost:${HOST_PORT_MASTER_RPC}"
echo "   Submit jobs:         spark://localhost:${HOST_PORT_MASTER_RPC}"
echo ""
echo "📝 Quick Test Commands:"
echo "   1. Check Master logs:    docker compose -p ${PROJECT_NAME} logs -f spark-master"
echo "   2. Submit test job:      docker exec ${CONTAINER_PREFIX}-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100"
echo "   3. Enter Master shell:   docker exec -it ${CONTAINER_PREFIX}-spark-master bash"
echo "   4. Check cluster status: curl -s http://localhost:${HOST_PORT_MASTER_UI} | grep -o 'Alive Workers: [0-9]*'"
echo ""
echo "==========================================="

# 等待并检查Master状态
sleep 10
echo -n "Checking Master Web UI: "
if curl -s "http://localhost:${HOST_PORT_MASTER_UI}" > /dev/null 2>&1; then
    echo "✅ Accessible"
else
    echo "⚠️  Not accessible yet (might need more time)"
fi
