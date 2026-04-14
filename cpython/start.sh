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

# 参数解析
IMAGE_NAME=""
PROJECT_NAME="cpython"
CONTAINER_NAME="cpython"

usage() {
    echo "Usage: ./start.sh <image_name> [--project-name <name>]"
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
        *)
            echo "❌ Unknown option: $1"
            usage
            ;;
    esac
done

if [ "$PROJECT_NAME" != "cpython" ]; then
    CONTAINER_NAME="${PROJECT_NAME}-cpython"
fi

export IMAGE_NAME
export PROJECT_NAME
export CONTAINER_NAME

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

echo "🚀 Starting cpython container..."
echo "   Project name   : $PROJECT_NAME"
echo "   Container name : $CONTAINER_NAME"
docker compose -p "$PROJECT_NAME" down --remove-orphans
docker compose -p "$PROJECT_NAME" up -d

# 显示状态
echo "📊 status:"
docker compose -p "$PROJECT_NAME" ps
