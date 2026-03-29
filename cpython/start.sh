#!/bin/bash

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

docker compose down --remove-orphans
docker compose up -d

# 显示状态
echo "📊 status:"
docker compose ps