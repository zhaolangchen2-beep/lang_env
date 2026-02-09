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

# 接收 deploy.sh 传来的镜像名
if [ -z "$1" ]; then
    echo "Usage: ./start.sh <image_name>"
    exit 1
fi

export IMAGE_NAME=$1

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
docker compose down --remove-orphans
docker compose --env-file .env  up -d

echo "⏳ Waiting for services to start (40 seconds)..."
sleep 40

# 显示状态
echo "📊 Cluster status:"
docker compose ps

# 显示访问信息
echo ""
echo "==========================================="
echo "✅ Spark Cluster Started Successfully!"
echo "==========================================="
echo ""
echo "🌐 Web Interfaces:"
echo "   Spark Master UI:     http://localhost:8080"
echo "   Spark Worker 1 UI:   http://localhost:8081"
echo "   Spark Worker 2 UI:   http://localhost:8082"
echo "   Spark History UI:    http://localhost:18080"
echo "   Jupyter Notebook:    http://localhost:8888"
echo ""
echo "🔗 Connection URLs:"
echo "   Master URL:          spark://localhost:7077"
echo "   Submit jobs:         spark://localhost:7077"
echo ""
echo "📝 Quick Test Commands:"
echo "   1. Check Master logs:    docker-compose logs -f spark-master"
echo "   2. Submit test job:      docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100"
echo "   3. Enter Master shell:   docker exec -it spark-master bash"
echo "   4. Check cluster status: curl -s http://localhost:8080 | grep -o 'Alive Workers: [0-9]*'"
echo ""
echo "==========================================="

# 等待并检查Master状态
sleep 10
echo -n "Checking Master Web UI: "
if curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo "✅ Accessible"
else
    echo "⚠️  Not accessible yet (might need more time)"
fi
