#!/bin/bash
# ================================================================
# deploy.sh — 一键部署脚本
#
# 功能流程：
#   Step 1   : 检查宿主机环境（代理、Docker、基础镜像）
#   Step 2   : 根据模块 dependency.yaml 下载依赖软件包
#   Step 2.5 : 在容器内编译通用 Python 发行版
#   Step 3   : 构建模块 Docker 镜像
#   Step 4   : 拉起容器
#
# 用法：
#   ./deploy.sh -m <module_name> -v <python_version>
#
# 示例：
#   ./deploy.sh -m spark -v 3.11.9
#
# 依赖文件：
#   conf.yaml                  — 全局配置（代理、Docker 版本、Python 编译参数等）
#   url.yaml                   — 所有离线资源的下载地址
#   <module>/dependency.yaml   — 模块级依赖声明
#   <module>/Dockerfile        — 模块镜像构建文件
#   <module>/start.sh          — 模块容器启动脚本
# ================================================================

set -e

# ────────────────────────────────────────────────────────────────
# 全局路径常量
# ────────────────────────────────────────────────────────────────
CONF_FILE="conf.yaml"
URL_FILE="url.yaml"
TMP_DIR="$(pwd)/tmp"
mkdir -p "$TMP_DIR"
DOCKER_RESTART_REQUIRED=false
# ────────────────────────────────────────────────────────────────
# 辅助函数
# ────────────────────────────────────────────────────────────────

# get_yaml_val <yaml_file> <python_subscript>
#   用宿主机 Python3 + PyYAML 解析 YAML，返回指定路径的值。
#   示例: get_yaml_val conf.yaml "['global']['proxy']"
get_yaml_val() {
    python3 -c \
        "import yaml; data = yaml.safe_load(open('$1')); print(data$2)" \
        2>/dev/null
}

write_file_if_changed() {
    local target_file="$1"
    local tmp_file
    tmp_file=$(mktemp)
    cat > "$tmp_file"

    if [ -f "$target_file" ] && cmp -s "$tmp_file" "$target_file"; then
        rm -f "$tmp_file"
        return 1
    fi

    mkdir -p "$(dirname "$target_file")"
    mv "$tmp_file" "$target_file"
    return 0
}

configure_docker_daemon() {
    if write_file_if_changed /etc/docker/daemon.json << 'EOF'
{
  "dns": ["8.8.8.8", "114.114.114.114"],
  "insecure-registries": ["ghcr.io", "ghcr.nju.edu.cn"]
}
EOF
    then
        DOCKER_RESTART_REQUIRED=true
        echo " -> Updated /etc/docker/daemon.json"
    else
        echo " -> /etc/docker/daemon.json unchanged"
    fi
}

configure_docker_service_proxy() {
    local proxy_value="$1"

    if [ -n "$proxy_value" ] && [ "$proxy_value" != "None" ]; then
        if write_file_if_changed /etc/systemd/system/docker.service.d/http-proxy.conf << EOF
[Service]
Environment="HTTP_PROXY=${proxy_value}"
Environment="HTTPS_PROXY=${proxy_value}"
Environment="NO_PROXY=localhost,127.0.0.1"
EOF
        then
            DOCKER_RESTART_REQUIRED=true
            echo " -> Updated Docker service proxy configuration"
        else
            echo " -> Docker service proxy configuration unchanged"
        fi
    else
        if [ -f /etc/systemd/system/docker.service.d/http-proxy.conf ]; then
            rm -f /etc/systemd/system/docker.service.d/http-proxy.conf
            DOCKER_RESTART_REQUIRED=true
            echo " -> Removed Docker service proxy configuration"
        else
            echo " -> No Docker service proxy configuration to remove"
        fi
    fi
}

render_prebuilt_image() {
    python3 - "$CONF_FILE" "$1" "$2" << 'PY'
import sys
import yaml

conf_file, arch, py_ver = sys.argv[1:4]
with open(conf_file, encoding="utf-8") as f:
    data = yaml.safe_load(f) or {}

template = ((data.get("python_build") or {}).get("prebuilt_image_template") or "").strip()
if not template:
    sys.exit(0)

parts = py_ver.split(".")
python_mm = ""
if len(parts) >= 2:
    python_mm = f"{parts[0]}{parts[1]}"

print(
    template.format(
        arch=arch,
        python_version=py_ver,
        python_mm=python_mm,
    )
)
PY
}

download_with_error() {
    local download_url="$1"
    local output_dir="$2"
    local display_name="$3"
    local log_file

    if [ -z "$download_url" ] || [ "$download_url" = "None" ]; then
        echo "❌ Error: Missing download URL for $display_name"
        exit 1
    fi

    log_file=$(mktemp)
    if wget --no-check-certificate -P "$output_dir" "$download_url" >"$log_file" 2>&1; then
        rm -f "$log_file"
        return 0
    fi

    echo "❌ Download failed: $display_name"
    echo "   URL      : $download_url"
    echo "   Output   : $output_dir"
    echo "   Details  :"
    tail -n 10 "$log_file" | sed 's/^/     /'
    rm -f "$log_file"
    exit 1
}

# ================================================================
#  参数解析
# ================================================================
MODULE_NAME=""
PYTHON_VERSION=""
COMPACT_MODE=false   # 默认关闭：使用镜像分层模式
                     # 开启后：仅复制编译产物，不保留中间镜像（节省空间）
SKIP_PY_BUILD=false
SKIP_PY_PKG=""  # 存储本地 Python 镜像 tar 包路径
# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--module)
            MODULE_NAME="$2"
            shift 2
            ;;
        -v|--version)
            PYTHON_VERSION="$2"
            shift 2
            ;;
		--skip-py-build|--skip-build)
            SKIP_PY_BUILD=true
            if [[ $# -gt 1 && ! "$2" =~ ^- ]]; then
                SKIP_PY_PKG="$2"
                shift 2
            else
                shift
            fi
            ;;
        --compact)
            COMPACT_MODE=true
            shift
            ;;
        -h|--help)
            echo "Usage: ./deploy.sh [OPTIONS] -m <module_name> -v <python_version>"
            echo ""
            echo "Options:"
            echo "  -m, --module <name>"
            echo "               Target module name, e.g. pyflink / pyspark / cpython."
            echo "  -v, --version <version>"
            echo "               Target Python version."
            echo "  --skip-build [tar] / --skip-py-build [tar]"
            echo "               Skip local Python build."
            echo "               With tar: docker load local image tar package."
            echo "               Without tar: docker pull prebuilt image from"
            echo "               python_build.prebuilt_image_template."
            echo "  --compact    Space-saving mode: only copy Python binaries"
            echo "               into module image (no intermediate cpython image)."
            echo "               Default: build a cpython_base:<version> image first,"
            echo "               then use it as BASE_IMAGE for module build."
            echo ""
            echo "Examples:"
            echo "  ./deploy.sh -m pyspark -v 3.14.2"
            echo "  ./deploy.sh --compact -m pyspark -v 3.14.2"
            echo "  ./deploy.sh --skip-build -m pyspark -v 3.14.3"
            echo "  ./deploy.sh --skip-build /tmp/cpython.tar -m pyspark -v 3.14.3"
            exit 0
            ;;
        -*)
            echo "❌ Unknown option: $1"
            echo "   Run './deploy.sh --help' for usage."
            exit 1
            ;;
        *)
            echo "❌ Unexpected positional argument: $1"
            echo "   Use -m <module_name> -v <python_version>."
            exit 1
            ;;
    esac
done

# 校验必需参数
if [ -z "$MODULE_NAME" ] || [ -z "$PYTHON_VERSION" ]; then
    echo "❌ Missing required arguments."
    echo "   Usage: ./deploy.sh [OPTIONS] -m <module_name> -v <python_version>"
    exit 1
fi

MODULE_DIR="./${MODULE_NAME}"
DEP_FILE="${MODULE_DIR}/dependency.yaml"

if [ ! -f "$DEP_FILE" ]; then
    echo "❌ Error: $DEP_FILE not found. Is '$MODULE_NAME' a valid module?"
    exit 1
fi

echo " -> Module        : $MODULE_NAME"
echo " -> Python version: $PYTHON_VERSION"
if [ "$COMPACT_MODE" = true ]; then
    echo " -> Build mode    : compact (binary-copy only, saves space)"
else
    echo " -> Build mode    : layered (cpython_base image, keeps Python in path)"
fi

# ================================================================
#  Step 1 : 宿主机环境检查
# ================================================================
echo "Step 1: Checking Host Environment..."

# ── 1.1 主机python3检查 ────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo ""
    echo "❌ Error: python3 not found on this system."
    echo ""
    echo "   This script requires Python 3 with the PyYAML package"
    echo "   to parse configuration files (conf.yaml, url.yaml)."
    echo ""
    echo "   Please install Python 3 first:"
    echo ""
    echo "     # CentOS / openEuler / RHEL:"
    echo "     yum install -y python3"
    echo ""
    echo "     # Ubuntu / Debian:"
    echo "     apt-get install -y python3"
    echo ""
    exit 1
fi

# ── 1.2 pyyaml检查 ────────────────────────────────────────────────
if ! python3 -c "import yaml" &>/dev/null; then
    echo ""
    echo "❌ Error: Python module 'PyYAML' is not installed."
    echo ""
    echo "   This script requires PyYAML to parse YAML configuration files."
    echo ""
    echo "   Please install it using one of the following methods:"
    echo ""
    echo "     # Via pip (recommended):"
    echo "     python3 -m pip install pyyaml"
    echo ""
    echo "     # Via pip (if pip not in PATH):"
    echo "     python3 -m ensurepip --default-pip && python3 -m pip install pyyaml"
    echo ""
    echo "     # Via system package manager:"
    echo "     # CentOS / openEuler / RHEL:"
    echo "     yum install -y python3-pyyaml"
    echo ""
    echo "     # Ubuntu / Debian:"
    echo "     apt-get install -y python3-yaml"
    echo ""
    exit 1
fi

echo " -> Python3 : $(python3 --version)"
echo " -> PyYAML  : $(python3 -c "import yaml; print(yaml.__version__)" 2>/dev/null || echo 'version unknown')"
echo " -> Prerequisites OK."

# ── 1.3 代理配置 ────────────────────────────────────────────────
PROXY=$(get_yaml_val $CONF_FILE "['global']['proxy']")
if [ "$PROXY" != "None" ] && [ -n "$PROXY" ]; then
    export http_proxy=$PROXY
    export https_proxy=$PROXY
    echo " -> Proxy set to $PROXY"
fi

# ── 1.4 Docker 版本检查 / 安装 ─────────────────────────────────
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
    # ---- 卸载旧版本 Docker（如果存在） ----
    # 涵盖各种历史包名：docker / docker-engine / docker.io / docker-ce 等
    if [ "$DOCKER_VER" != "0.0.0" ]; then
        echo " -> Removing old Docker ($DOCKER_VER)..."

        # 先停止服务，忽略不存在的情况
        systemctl stop docker.socket 2>/dev/null || true
        systemctl stop docker        2>/dev/null || true
        systemctl stop containerd    2>/dev/null || true

        # 卸载所有可能的 Docker 相关包
        yum remove -y \
            docker \
            docker-client \
            docker-client-latest \
            docker-common \
            docker-latest \
            docker-latest-logrotate \
            docker-logrotate \
            docker-engine \
            docker-ce \
            docker-ce-cli \
            docker-ce-rootless-extras \
            docker-buildx-plugin \
            docker-compose-plugin \
            containerd.io \
            2>/dev/null || true

        echo " -> Old Docker packages removed."
    fi
    # 写入 Repo 配置 (注意：这里使用 EOF 包装，并转义了 $basearch)
    cat > /etc/yum.repos.d/docker-ce.repo << EOF
[docker-ce-stable]
name=Docker CE Stable - \$basearch
baseurl=https://mirrors.aliyun.com/docker-ce/linux/centos/8/\$basearch/stable
enabled=1
gpgcheck=1
gpgkey=https://mirrors.aliyun.com/docker-ce/linux/centos/gpg

EOF

    # 配置 yum 容错
    if ! grep -q "sslverify=False" /etc/yum.conf; then
        echo "sslverify=False" >> /etc/yum.conf
    fi

    # 安装指定版本
    echo " -> Running yum install..."
    yum install -y docker-ce-3:$EXPECTED_VER-1.el8
    DOCKER_RESTART_REQUIRED=true
    
    echo "✅ Docker upgraded successfully."
else
    echo " -> Docker version check passed."
fi

echo " -> Configuring Docker daemon..."
configure_docker_daemon
configure_docker_service_proxy "$PROXY"

if [ "$DOCKER_RESTART_REQUIRED" = true ]; then
    echo " -> Reloading and restarting Docker service..."
    systemctl daemon-reload
    systemctl enable docker
    systemctl restart docker
    echo " -> Docker daemon configuration applied."
else
    echo " -> Docker configuration unchanged. Skipping Docker restart."
fi

# ================================================================
#  Step 2 : 模块依赖软件包下载
# ================================================================
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
        download_with_error "$DOWNLOAD_URL" "$TMP_DIR" "$DEP"
    fi
done
# ── 1.3 基础操作系统镜像 ───────────────────────────────────────
echo " -> Checking Base OS Image..."

BASE_OS=$(get_yaml_val "$CONF_FILE" "['global']['base_os_image']")
# ── 1.3 基础操作系统镜像 ───────────────────────────────────────
if [ "$SKIP_PY_BUILD" = true ] && [ -n "$SKIP_PY_PKG" ]; then
    echo " -> [SKIP] Skipping Base OS check because --skip-py-build is provided."

elif [[ "$(docker images -q "$BASE_OS" 2>/dev/null)" == "" ]]; then
    echo " -> Base image $BASE_OS not found locally. Preparing offline load..."

    # 识别宿主机 CPU 架构，映射到 url.yaml 中的 key
    ARCH=$(uname -m)
    echo " -> Detected architecture: $ARCH"

    case "$ARCH" in
        x86_64)  ARCH_KEY="x86"     ;;
        aarch64) ARCH_KEY="aarch64" ;;
        *)
            echo "❌ Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac

    # 从 url.yaml 解析出文件名和下载地址
    #   结构示例:
    #     urls:
    #       base_images_x86:
    #         openeuler-22.03-x86.tar.gz: https://...
    IMG_INFO=$(python3 -c "
import yaml
with open('$URL_FILE') as f:
    data = yaml.safe_load(f)['urls']['base_images_${ARCH_KEY}']
    filename = list(data.keys())[0]
    url = data[filename]
    print(f'{filename}|{url}')
")

    IMG_FILENAME=$(echo "$IMG_INFO" | cut -d'|' -f1)
    IMG_URL=$(echo "$IMG_INFO" | cut -d'|' -f2)
    IMG_PATH="$TMP_DIR/$IMG_FILENAME"

    # 下载镜像离线包（如已存在则跳过）
    if [ ! -f "$IMG_PATH" ]; then
        echo " -> Downloading $IMG_FILENAME ..."
        download_with_error "$IMG_URL" "$TMP_DIR" "$IMG_FILENAME"
    else
        echo " -> Offline image package already cached."
    fi

    # 加载镜像到 Docker
    echo " -> docker load -i $IMG_PATH"
    LOAD_RESULT=$(docker load -i "$IMG_PATH")
    echo "$LOAD_RESULT"

    # 给加载出的镜像打上 conf.yaml 中定义的标签，确保后续引用一致
    LOADED_ID=$(echo "$LOAD_RESULT" | grep -oE '[0-9a-f]{12}' | head -n1)
    if [ -z "$LOADED_ID" ]; then
        LOADED_ID=$(echo "$LOAD_RESULT" | awk -F ': ' '/Loaded image/ {print $2}')
    fi

    if [ -n "$LOADED_ID" ]; then
        echo " -> Tagging $LOADED_ID → $BASE_OS"
        docker tag "$LOADED_ID" "$BASE_OS"
    else
        echo "❌ Error: Failed to parse base image ID or name from docker load output."
        echo "   Debug info: $LOAD_RESULT"
        exit 1
    fi
else
    echo " -> Base image $BASE_OS already exists."
fi
# ================================================================
#  Step 2.5 : Python 环境准备
#
#  两种模式：
#
#  【默认模式 — 镜像分层】
#    在基础 OS 镜像上编译 Python，生成中间镜像 cpython_base:<version>。
#    后续模块 Dockerfile 以 cpython_base:<version> 为 BASE_IMAGE 构建。
#    优点：模块镜像内保留完整 Python 安装路径，python3 直接可用。
#
#  【--compact 模式 — 二进制复制】
#    在临时容器内编译 Python，产物输出到宿主机 tmp/ 目录。
#    后续模块 Dockerfile 用 COPY 将二进制复制进去。
#    优点：不产生中间镜像，最终镜像体积更小。
# ================================================================
echo "Step 2.5: Preparing Generic Python Environment (Containerized Build)..."

# ── 2.5.1 校验 Python 版本是否在支持列表中 ────────────────────
SUPPORTED_VERSIONS=$(python3 -c "
import yaml
print(' '.join(yaml.safe_load(open('$CONF_FILE'))['python_build']['supported_versions']))
")

if [[ ! " $SUPPORTED_VERSIONS " =~ " $PYTHON_VERSION " ]]; then
    echo "❌ Error: Python $PYTHON_VERSION not in supported list: $SUPPORTED_VERSIONS"
    exit 1
fi

# ── 2.5.2 按模式分别处理 ─────────────────────────────────────

if [ "$COMPACT_MODE" = true ]; then
    CPYTHON_IMAGE="cpython_compact:${PYTHON_VERSION}"
else
    CPYTHON_IMAGE="cpython_full:${PYTHON_VERSION}"
fi

if [ "$SKIP_PY_BUILD" = true ] && [ -n "$SKIP_PY_PKG" ]; then
    # ── 场景 A: 离线镜像导入模式 ──────────────────────────────
    echo " -> [OFFLINE MODE] Skipping Python source download and build."
    echo " -> Loading image from: $SKIP_PY_PKG"

    if [ ! -f "$SKIP_PY_PKG" ]; then
        echo "❌ Error: File $SKIP_PY_PKG not found."
        exit 1
    fi

	# 执行加载并捕捉输出
    LOAD_OUT=$(docker load -i "$SKIP_PY_PKG")
    echo "$LOAD_OUT"

    # 优先尝试匹配 12 位 ID (十六进制)
    NEW_ID=$(echo "$LOAD_OUT" | grep -oE '[0-9a-f]{12}' | head -n1)

    # 如果没匹配到 ID，尝试从 "Loaded image: repository:tag" 中提取名称
    if [ -z "$NEW_ID" ]; then
        # 提取 "Loaded image: " 之后的内容
        NEW_ID=$(echo "$LOAD_OUT" | awk -F ': ' '/Loaded image/ {print $2}')
    fi

    if [ -n "$NEW_ID" ]; then
        echo " -> Successfully identified source: $NEW_ID"
        echo " -> Tagging $NEW_ID as $CPYTHON_IMAGE"
        docker tag "$NEW_ID" "$CPYTHON_IMAGE"
    else
        echo "❌ Error: Failed to parse Image ID or Name from docker load output."
        echo "   Debug info: $LOAD_OUT"
        exit 1
    fi

elif [ "$SKIP_PY_BUILD" = true ]; then
    echo " -> [REGISTRY MODE] Skipping local Python build."

    ARCH=$(uname -m)
    PREBUILT_IMAGE=$(render_prebuilt_image "$ARCH" "$PYTHON_VERSION")

    if [ -z "$PREBUILT_IMAGE" ]; then
        echo "❌ Error: python_build.prebuilt_image_template is not configured in $CONF_FILE."
        exit 1
    fi

    echo " -> Pulling prebuilt Python image: $PREBUILT_IMAGE"
    if docker pull "$PREBUILT_IMAGE"; then
        echo " -> Tagging $PREBUILT_IMAGE as $CPYTHON_IMAGE"
        docker tag "$PREBUILT_IMAGE" "$CPYTHON_IMAGE"
    else
        echo "❌ Error: Failed to pull prebuilt Python image: $PREBUILT_IMAGE"
        echo "   If this is a private image, please run: docker login ghcr.io"
        exit 1
    fi

else
	# ── 2.5.2 读取编译参数 ────────────────────────────────────────
	CFLAGS_VAL=$(get_yaml_val "$CONF_FILE" "['python_build']['cflags']")
	LDFLAGS_VAL=$(get_yaml_val "$CONF_FILE" "['python_build']['ldflags']")
	CONFIG_ARGS=$(get_yaml_val "$CONF_FILE" "['python_build']['configure_args']")
	PYTHON_INSTALL_PREFIX=$(get_yaml_val "$CONF_FILE" "['python_build']['install_prefix']")
	if [ "$PYTHON_INSTALL_PREFIX" = "None" ] || [ -z "$PYTHON_INSTALL_PREFIX" ]; then
		PYTHON_INSTALL_PREFIX=$(get_yaml_val "$CONF_FILE" "['python_build']['install_dir']")
	fi
	if [ "$PYTHON_INSTALL_PREFIX" = "None" ] || [ -z "$PYTHON_INSTALL_PREFIX" ]; then
		PYTHON_INSTALL_PREFIX="/opt/python"
	fi

	# ── 2.5.3 确保 Python 源码包已下载 ───────────────────────────
	PY_SOURCE_URL=$(get_yaml_val "$URL_FILE" "['urls']['python_sources']['$PYTHON_VERSION']")
	PY_FILENAME=$(basename "$PY_SOURCE_URL")

	if [ ! -f "$TMP_DIR/$PY_FILENAME" ]; then
		echo " -> Downloading Python source: $PY_FILENAME"
		download_with_error "$PY_SOURCE_URL" "$TMP_DIR" "$PY_FILENAME"
	else
		echo " -> Python source already cached: $PY_FILENAME"
	fi

	if [[ "$(docker images -q "$CPYTHON_IMAGE" 2>/dev/null)" != "" ]]; then
		echo " -> Image $CPYTHON_IMAGE already exists. Skipping build."
	else
		echo " -> Building $CPYTHON_IMAGE ..."

		echo "    Mode             = $([ "$COMPACT_MODE" = true ] && echo 'compact (multi-stage, small)' || echo 'default (single-stage, with build tools)')"
		echo "    BASE_OS          = $BASE_OS"
		echo "    INSTALL_PREFIX   = $PYTHON_INSTALL_PREFIX"
		echo "    CFLAGS           = $CFLAGS_VAL"
		echo "    LDFLAGS          = $LDFLAGS_VAL"
		echo "    configure args   = $CONFIG_ARGS"

		CPYTHON_DOCKERFILE="$TMP_DIR/Dockerfile.cpython_base"

		if [ "$COMPACT_MODE" = true ]; then
			# ── compact: 多阶段构建，最终镜像不含编译工具 ──
			cat > "$CPYTHON_DOCKERFILE" << 'DOCKERFILE_EOF'
ARG BASE_IMAGE

# ======== Stage 1: 编译 ========
FROM ${BASE_IMAGE} AS builder

ARG PYTHON_VERSION
ARG PYTHON_INSTALL_PREFIX
ARG CFLAGS_VAL
ARG LDFLAGS_VAL
ARG CONFIG_ARGS
ARG PROXY
ARG PY_FILENAME

ENV http_proxy=${PROXY} \
	https_proxy=${PROXY} \
	CFLAGS=${CFLAGS_VAL} \
	LDFLAGS=${LDFLAGS_VAL}

RUN echo "sslverify=false" >> /etc/yum.conf && \
	yum clean all && yum makecache && \
	yum install -y \
		gcc gcc-c++ make \
		findutils diffutils file \
		tar gzip \
		zlib-devel bzip2-devel openssl-devel \
		ncurses-devel sqlite-devel readline-devel \
		tk-devel gdbm-devel libpcap-devel \
		xz-devel libffi-devel libuuid-devel \
		libzstd-devel && \
	yum clean all

COPY tmp/${PY_FILENAME} /tmp/src/${PY_FILENAME}
RUN cd /tmp/src && \
	tar xf ${PY_FILENAME} && \
	cd Python-${PYTHON_VERSION} && \
	./configure --prefix=${PYTHON_INSTALL_PREFIX} ${CONFIG_ARGS} > /dev/null && \
	make -j$(nproc) > /dev/null && \
	make install > /dev/null && \
	rm -rf /tmp/src

# ======== Stage 2: 仅保留运行时 ========
FROM ${BASE_IMAGE}

ARG PYTHON_VERSION
ARG PYTHON_INSTALL_PREFIX
ARG PROXY
ENV http_proxy=${PROXY} \
	https_proxy=${PROXY} \
RUN echo "sslverify=false" >> /etc/yum.conf && \
	yum clean all && yum makecache && \
	yum install -y \
		zlib bzip2-libs openssl-libs \
		ncurses-libs sqlite-libs readline \
		libffi xz-libs libuuid libzstd && \
	yum clean all

COPY --from=builder ${PYTHON_INSTALL_PREFIX} ${PYTHON_INSTALL_PREFIX}

ENV PATH="${PYTHON_INSTALL_PREFIX}/bin:${PATH}" \
	LD_LIBRARY_PATH="${PYTHON_INSTALL_PREFIX}/lib:${LD_LIBRARY_PATH}"

RUN python3 --version && pip3 --version

LABEL description="CPython ${PYTHON_VERSION} compact (runtime only)" \
	  python.version="${PYTHON_VERSION}" \
	  python.prefix="${PYTHON_INSTALL_PREFIX}"
DOCKERFILE_EOF

		else
			# ── default: 单阶段构建，保留编译工具链 ──
			cat > "$CPYTHON_DOCKERFILE" << 'DOCKERFILE_EOF'
ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG PYTHON_VERSION
ARG PYTHON_INSTALL_PREFIX
ARG CFLAGS_VAL
ARG LDFLAGS_VAL
ARG CONFIG_ARGS
ARG PROXY
ARG PY_FILENAME

ENV http_proxy=${PROXY} \
	https_proxy=${PROXY} \
	CFLAGS=${CFLAGS_VAL} \
	LDFLAGS=${LDFLAGS_VAL}

RUN echo "sslverify=false" >> /etc/yum.conf && \
	yum clean all && yum makecache && \
	yum install -y \
		gcc gcc-c++ make \
		findutils diffutils file \
		tar gzip \
		zlib-devel bzip2-devel openssl-devel \
		ncurses-devel sqlite-devel readline-devel \
		tk-devel gdbm-devel libpcap-devel \
		xz-devel libffi-devel libuuid-devel \
		libzstd-devel && \
	yum clean all

COPY tmp/${PY_FILENAME} /tmp/src/${PY_FILENAME}
RUN cd /tmp/src && \
	tar xf ${PY_FILENAME} && \
	cd Python-${PYTHON_VERSION} && \
	./configure --prefix=${PYTHON_INSTALL_PREFIX} ${CONFIG_ARGS} > /dev/null && \
	make -j$(nproc) > /dev/null && \
	make install > /dev/null && \
	rm -rf /tmp/src

ENV PATH="${PYTHON_INSTALL_PREFIX}/bin:${PATH}" \
	LD_LIBRARY_PATH="${PYTHON_INSTALL_PREFIX}/lib:${LD_LIBRARY_PATH}"

RUN python3 --version && pip3 --version

LABEL description="CPython ${PYTHON_VERSION} full (with build tools)" \
	  python.version="${PYTHON_VERSION}" \
	  python.prefix="${PYTHON_INSTALL_PREFIX}"
DOCKERFILE_EOF
		fi

		# 执行构建（以项目根目录为 context，确保 tmp/ 可访问）
		docker build \
			-f "$CPYTHON_DOCKERFILE" \
			--build-arg BASE_IMAGE="$BASE_OS" \
			--build-arg PYTHON_VERSION="$PYTHON_VERSION" \
			--build-arg PYTHON_INSTALL_PREFIX="$PYTHON_INSTALL_PREFIX" \
			--build-arg CFLAGS_VAL="$CFLAGS_VAL" \
			--build-arg LDFLAGS_VAL="$LDFLAGS_VAL" \
			--build-arg CONFIG_ARGS="$CONFIG_ARGS" \
			--build-arg PROXY="$PROXY" \
			--build-arg PY_FILENAME="$PY_FILENAME" \
			-t "$CPYTHON_IMAGE" \
			.
		echo "✅ Python image $CPYTHON_IMAGE built successfully."
	fi
fi
MODULE_BASE_IMAGE="$CPYTHON_IMAGE"

# ================================================================
#  Step 3 : 构建模块 Docker 镜像
# ================================================================
FULL_IMAGE_NAME="${MODULE_NAME}:${PYTHON_VERSION}"
echo ""
echo "Step 3: Building Docker image '$FULL_IMAGE_NAME'..."
echo " -> BASE_IMAGE = $MODULE_BASE_IMAGE"

if [ "$COMPACT_MODE" = true ]; then
    echo " -> [compact] Module Dockerfile should COPY from tmp/python_versions/${PYTHON_VERSION}/"
else
    echo " -> [layered] Module Dockerfile inherits FROM $MODULE_BASE_IMAGE (python3 already in PATH)"
fi

# 以项目根目录为 build context
# Dockerfile 可使用的 ARG：
#   BASE_IMAGE       — 默认模式: cpython_base:<ver>  /  compact模式: 原始OS镜像
#   PYTHON_VERSION   — Python 版本号
#   PROXY            — 代理地址
#   COMPACT_MODE     — "true" 或 "false"，Dockerfile 内可据此决定是否 COPY 二进制
docker build \
    -f "$MODULE_DIR/Dockerfile" \
    --build-arg BASE_IMAGE="$MODULE_BASE_IMAGE" \
    --build-arg PYTHON_VERSION="$PYTHON_VERSION" \
    --build-arg PROXY="$PROXY" \
    --build-arg COMPACT_MODE="$COMPACT_MODE" \
    -t "$FULL_IMAGE_NAME" \
    .

echo "✅ Image '$FULL_IMAGE_NAME' built successfully."


# ================= 第四步：拉起容器 =================
echo "Step 4: Executing $MODULE_DIR/start.sh..."
cd "$MODULE_DIR"
chmod +x start.sh

# 将镜像名作为第一个参数传入
./start.sh "$FULL_IMAGE_NAME"
