#!/bin/bash
set -e  # 遇到错误立即退出

# ================= 辅助函数：YAML解析 =================
# 使用系统自带Python解析YAML，避免依赖 jq/yq
function get_yaml_val() {
    python3 -c "import yaml; data = yaml.safe_load(open('$1')); print(data$2)" 2>/dev/null
}

# ================= 第一步：环境检查 =================
CONF_FILE="conf.yaml"
URL_FILE="url.yaml"
TMP_DIR="$(pwd)/tmp"

echo "Step 1: Checking Host Environment..."

# 1. 检查 Proxy
PROXY=$(get_yaml_val $CONF_FILE "['global']['proxy']")
if [ "$PROXY" != "None" ] && [ -n "$PROXY" ]; then
    export http_proxy=$PROXY
    export https_proxy=$PROXY
    echo " -> Proxy set to $PROXY"
fi

# 2. 获取docker版本
if ! command -v docker &> /dev/null; then
    DOCKER_VER="0.0.0"
    echo " -> Docker not found. Installing..."
else
    # 提取版本号，例如 26.1.3
    DOCKER_VER=$(docker --version | awk '{print $3}' | tr -d ',')
fi

EXPECTED_VER=$(get_yaml_val $CONF_FILE "['global']['docker_min_version']")
echo " -> Current Docker: $DOCKER_VER (Required >= $EXPECTED_VER)"

IS_LOWER=$(printf '%s\n%s' "$EXPECTED_VER" "$DOCKER_VER" | sort -V | head -n1)

if [ "$IS_LOWER" != "$EXPECTED_VER" ] || [ "$DOCKER_VER" == "0.0.0" ]; then
    echo "⚠️ Docker version is too low or not installed. Upgrading to $EXPECTED_VER..."
    
    # 写入 Repo 配置 (注意：这里使用 EOF 包装，并转义了 $basearch)
    cat > /etc/yum.repos.d/docker-ce.repo << EOF
[docker-ce-stable]
name=Docker CE Stable - \$basearch
baseurl=https://download.docker.com/linux/centos/8/\$basearch/stable
enabled=1
gpgcheck=1
gpgkey=https://download.docker.com/linux/centos/gpg

[docker-ce-stable-debuginfo]
name=Docker CE Stable - Debuginfo \$basearch
baseurl=https://download.docker.com/linux/centos/8/debug-\$basearch/stable
enabled=0
gpgcheck=1
gpgkey=https://download.docker.com/linux/centos/gpg

[docker-ce-stable-source]
name=Docker CE Stable - Sources
baseurl=https://download.docker.com/linux/centos/8/source/stable
enabled=0
gpgcheck=1
gpgkey=https://download.docker.com/linux/centos/gpg
EOF

    # 配置 yum 容错
    if ! grep -q "sslverify=False" /etc/yum.conf; then
        echo "sslverify=False" >> /etc/yum.conf
    fi

    # 安装指定版本
    echo " -> Running yum install..."
    yum install -y docker-ce-3:$EXPECTED_VER-1.el8

    # 重启服务
    systemctl daemon-reload
    systemctl enable docker
    systemctl restart docker
    
    echo "✅ Docker upgraded successfully."
else
    echo " -> Docker version check passed."
fi


# 3. 检查基础镜像
echo "Step 3: Checking Base OS Image..."

BASE_OS=$(get_yaml_val $CONF_FILE "['global']['base_os_image']")

if [[ "$(docker images -q $BASE_OS 2> /dev/null)" == "" ]]; then
    echo " -> Base image $BASE_OS not found. Preparing offline download..."
    
    # 1. 识别主机架构
    ARCH=$(uname -m)
    echo " -> Detected Architecture: $ARCH"
    
    # 映射架构名称到 yaml 的 key
    case $ARCH in
        x86_64)  ARCH_KEY="x86" ;;
        aarch64) ARCH_KEY="aarch64" ;;
        *) echo "❌ Unsupported architecture: $ARCH"; exit 1 ;;
    esac

    # 2. 从 url.yaml 获取文件名和下载链接
    # 注意：这里假设 yaml 结构中 base_images_$ARCH_KEY 下只有一个子项
    # 我们通过 python 提取第一个 key (文件名) 和第一个 value (URL)
    IMG_INFO=$(python3 -c "
import yaml
with open('$URL_FILE') as f:
    data = yaml.safe_load(f)['urls']['base_images_$ARCH_KEY']
    filename = list(data.keys())[0]
    url = data[filename]
    print(f'{filename}|{url}')
")
    
    IMG_FILENAME=$(echo $IMG_INFO | cut -d'|' -f1)
    IMG_URL=$(echo $IMG_INFO | cut -d'|' -f2)
    
    IMG_PATH="$TMP_DIR/$IMG_FILENAME"

    # 3. 下载离线包
    if [ ! -f "$IMG_PATH" ]; then
        echo " -> Downloading $IMG_FILENAME from $IMG_URL..."
        wget --no-check-certificate -P "$TMP_DIR" "$IMG_URL"
    else
        echo " -> Offline image package already exists in tmp."
    fi

    # 4. 执行 docker load
    echo " -> Loading image into Docker..."
    LOAD_RESULT=$(docker load -i "$IMG_PATH")
    echo "$LOAD_RESULT"

    # 5. 自动打 Tag (可选)
    # 如果加载出来的镜像名字和 conf.yaml 里的 BASE_OS 不一致，需要打个 tag 关联起来
    # 通常 openEuler 离线包 load 出来后镜像名可能不带版本号，这里我们强制关联
    LOADED_ID=$(echo "$LOAD_RESULT" | grep -oE '[0-9a-f]{12}' | head -n1)
    if [ -n "$LOADED_ID" ]; then
        echo " -> Tagging image $LOADED_ID as $BASE_OS"
        docker tag "$LOADED_ID" "$BASE_OS"
    fi
else
    echo " -> Base image $BASE_OS exists."
fi

# ================= 第二步：依赖下载 =================
MODULE_NAME=$1
PYTHON_VERSION=$2

if [ -z "$MODULE_NAME" ] || [ -z "$PYTHON_VERSION" ]; then
    echo "Usage: ./deploy.sh <module_name> <python_version>"
    exit 1
fi

MODULE_DIR="./$MODULE_NAME"
DEP_FILE="$MODULE_DIR/dependency.yaml"

if [ ! -f "$DEP_FILE" ]; then
    echo "Error: Module $MODULE_NAME not found or dependency.yaml missing."
    exit 1
fi

echo "Step 2: Resolving Dependencies for $MODULE_NAME..."

# 读取该模块需要的软件列表 (假设 dependency.yaml 格式为 list)
# 这里简化处理，假设我们需要遍历 dependency.yaml 里的 keys
# 实际项目中可能需要更复杂的解析
DEPENDENCIES=$(python3 -c "import yaml; print(' '.join(yaml.safe_load(open('$DEP_FILE')).get('software', [])))")

for DEP in $DEPENDENCIES; do
    # 检查 tmp 下是否存在
    if [ -f "$TMP_DIR/$DEP" ]; then
        echo " -> $DEP found in tmp."
    else
        echo " -> $DEP missing. Checking url.yaml..."
        # 从 url.yaml 获取下载链接
        DOWNLOAD_URL=$(get_yaml_val $URL_FILE "['urls']['software']['$DEP']")
        
        if [ "$DOWNLOAD_URL" == "None" ]; then
             echo "Error: URL for $DEP not defined in url.yaml"
             exit 1
        fi
        
        echo " -> Downloading $DEP from $DOWNLOAD_URL..."
        wget --no-check-certificate -P "$TMP_DIR" "$DOWNLOAD_URL"
    fi
done


# ================= 特殊步骤：准备通用 Python 环境 =================
echo "Step 2.5: Preparing Generic Python Environment (Containerized Build)..."

# 1. 获取配置
SUPPORTED_VERSIONS=$(python3 -c "import yaml; print(' '.join(yaml.safe_load(open('$CONF_FILE'))['python_build']['supported_versions']))")
CFLAGS_VAL=$(get_yaml_val $CONF_FILE "['python_build']['cflags']")
CONFIG_ARGS=$(get_yaml_val $CONF_FILE "['python_build']['configure_args']")
BASE_OS=$(get_yaml_val $CONF_FILE "['global']['base_os_image']") # 确保使用和运行环境一致的基础镜像

# 2. 校验版本
if [[ ! " $SUPPORTED_VERSIONS " =~ " $PYTHON_VERSION " ]]; then
    echo "Error: Python version $PYTHON_VERSION is not in supported_versions list: $SUPPORTED_VERSIONS"
    exit 1
fi

# 3. 检查是否需要编译
PYTHON_DIST_HOST_DIR="$TMP_DIR/python_versions/$PYTHON_VERSION"
if [ -d "$PYTHON_DIST_HOST_DIR/bin" ]; then
    echo " -> Python $PYTHON_VERSION generic build found in $PYTHON_DIST_HOST_DIR. Skipping build."
else
    echo " -> Python $PYTHON_VERSION build missing."
    
    # 3.1 确保源码包已下载 (复用之前的下载逻辑，确保 tmp 下有 Python-x.x.x.tgz)
    # 这里的 Key 需要和 url.yaml 里的对应，假设是 python_sources 下的 Key
    PY_SOURCE_URL=$(get_yaml_val $URL_FILE "['urls']['python_sources']['$PYTHON_VERSION']")
    PY_FILENAME=$(basename "$PY_SOURCE_URL")
    
    if [ ! -f "$TMP_DIR/$PY_FILENAME" ]; then
        echo " -> Downloading Python Source from $PY_SOURCE_URL..."
        wget --no-check-certificate -P "$TMP_DIR" "$PY_SOURCE_URL"
    fi

    # 3.2 启动临时容器进行编译
    echo " -> Starting temporary build container ($BASE_OS)..."
    echo "    (This may take a while. CFLAGS: $CFLAGS_VAL)"

    # 创建输出目录
    mkdir -p "$PYTHON_DIST_HOST_DIR"

    # 获取当前用户ID，用于修正文件权限
    CURRENT_UID=$(id -u)
    CURRENT_GID=$(id -g)

    # 核心：Docker 编译命令
    # 1. 挂载 tmp 目录到容器 /build_ctx
    # 2. 传递 代理 和 编译参数
    # 3. 安装编译依赖 (针对 Debian/Ubuntu，如果是 Alpine 需要改为 apk add)
    docker run --rm \
        -v "$TMP_DIR:/build_ctx" \
        -e http_proxy="$PROXY" \
        -e https_proxy="$PROXY" \
        -e CFLAGS="$CFLAGS_VAL" \
        -e LDFLAGS="$(get_yaml_val $CONF_FILE "['python_build']['ldflags']")" \
        "$BASE_OS" \
        /bin/bash -c "
            set -e
            echo '=== [Container] Installing Build Dependencies ==='
            # 安装构建 Python 必须的最小依赖集
			echo "sslverify=false" >> /etc/yum.conf
			yum clean all && yum makecache

			yum install -y \
				gcc \
				gcc-c++ \
				make \
				findutils \
				diffutils \
				file \
				tar \
				gzip \
				zlib-devel \
				bzip2-devel \
				openssl-devel \
				ncurses-devel \
				sqlite-devel \
				readline-devel \
				tk-devel \
				gdbm-devel \
				libpcap-devel \
				xz-devel \
				libffi-devel \
				libuuid-devel \
				libzstd-devel \
				sudo
            echo '=== [Container] Extracting Source ==='
            cd /build_ctx
            tar xzf $PY_FILENAME
            cd Python-$PYTHON_VERSION

            echo '=== [Container] Configuring ==='
            ./configure --prefix=/build_ctx/python_versions/$PYTHON_VERSION $CONFIG_ARGS > /dev/null

            echo '=== [Container] Compiling (Jobs: $(nproc)) ==='
            make -j$(nproc) > /dev/null

            echo '=== [Container] Installing ==='
            make install > /dev/null
            
            echo '=== [Container] Cleaning up permissions ==='
            # 修正归属权，否则宿主机无法操作
            chown -R $CURRENT_UID:$CURRENT_GID /build_ctx/python_versions/$PYTHON_VERSION
            
            # 清理源码解压目录 (可选)
            # cd .. && rm -rf Python-$PYTHON_VERSION
        "

    if [ $? -eq 0 ]; then
        echo " -> Compilation Success! Artifacts stored in $PYTHON_DIST_HOST_DIR"
    else
        echo "Error: Compilation failed inside Docker container."
        exit 1
    fi
fi

# ================= 第三步：构建镜像 =================
FULL_IMAGE_NAME="${MODULE_NAME}:${PYTHON_VERSION}"
echo "Step 3: Building Image $FULL_IMAGE_NAME..."

# 1. 准备该模块需要的 Python 产物路径
PYTHON_SRC_DIR="$TMP_DIR/python_versions/$PYTHON_VERSION"

# 2. 发起构建
# 我们在主目录发起构建，这样 Dockerfile 就能通过相对路径访问到 tmp/python_versions
docker build \
    -f "$MODULE_DIR/Dockerfile" \
    --build-arg PROXY="$PROXY" \
    --build-arg PYTHON_VERSION="$PYTHON_VERSION" \
    --build-arg BASE_IMAGE="$BASE_OS" \
    -t "${MODULE_NAME}:${PYTHON_VERSION}" \
    .  # 注意这里的点，代表以主目录为上下文

# ================= 第四步：拉起容器 =================
echo "Step 4: Executing $MODULE_DIR/start.sh..."
cd "$MODULE_DIR"
chmod +x start.sh

# 将镜像名作为第一个参数传入
./start.sh "$FULL_IMAGE_NAME"
