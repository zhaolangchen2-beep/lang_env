# lang_env 自动化环境部署与测试框架

**lang_env** 是一个专为 Python 生态项目设计的自动化 Docker 环境部署与基准测试框架。

## 🚀 核心特性

* **架构自适应 (Arch-Aware)**
自动识别 `x86_64` / `aarch64` 架构，精准匹配华为云或官方镜像源，无需手动切换。
* **容器化预编译**
Python 编译过程在隔离容器中完成，产物持久化保存在主机。各业务模块部署时直接挂载或复制产物，确保测试环境一致性并大幅节省重复编译时间。
* **参数化优化**
通过 `conf.yaml` 统一注入 Python 版本、编译参数（如 PGO/LTO）、网络代理等配置。
* **解耦设计**
主框架只负责“基础设施”（网络、基础镜像、Python环境），具体的业务逻辑（Spark/Flink等）完全封装在子模块中。

---

## 📂 项目目录结构

```text
lang_test/
├── conf.yaml           # 全局配置中心（代理、Docker版本、编译参数）
├── url.yaml            # 资源索引（Python源码、基础镜像、组件包链接）
├── tmp/                # 资源池（存放下载包及预编译的 Python 产物）
├── deploy.sh           # 【主入口】环境部署脚本
├── test.sh             # 【主入口】测试驱动脚本
├── out/                # 测试结果转储（日志、性能数据、SVG图表）
└── sub_modules/        # 子模块目录
    ├── dependency.yaml # 模块私有依赖定义
    ├── Dockerfile      # 镜像构建文件
    ├── start.sh        # 集群/容器拉起逻辑
    └── test.sh         # 模块特有的 benchmark 逻辑

```

---

## 🛠️ 快速开始

### 1. 配置全局变量

修改 `conf.yaml` 文件，设置代理、Docker 版本要求及目标 Python 版本：

```yaml
global:
  # 为主机执行时配置代理，并且构建镜像时作为参数传递
  proxy: "http://90.91.56.202:3128"
  # 最低 docker 版本，如果不满足则会配置 yum 源并尝试升级
  docker_min_version: "26.1.3"
  # 基础 OS 镜像
  base_os_image: "openeuler-24.03-lts-sp1:latest"

python_build:
  # 允许测试的 Python 版本列表
  supported_versions:
    - "3.14.2"
    - "3.14.3"
  # 当 deploy.sh 使用 --skip-build 且未传入本地 tar 包时，
  # 会按模板填充架构和 Python 版本后直接 docker pull
  # 可用占位符：
  #   {arch}            -> aarch64 / x86_64
  #   {python_version}  -> 3.14.3
  #   {python_mm}       -> 314 (由 major.minor 组成)
  prebuilt_image_template: "ghcr.io/zhaolangchen2-beep/lang_env/oe2403sp1-devel-py{python_mm}-gcc14-{arch}:latest"
  # 预编译参数，用于 deploy.sh 生成通用 Python
  cflags: "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"
  ldflags: ""
  configure_args: "--enable-optimizations --with-lto"
  install_dir: "/tmp/lang_test/python_dist"

```

### 2. 一键部署

使用 `deploy.sh` 脚本进行环境构建与部署：

```bash
# 用法: ./deploy.sh <子模块名> <Python版本>
./deploy.sh pyflink 3.14.2

```

> **执行逻辑：**
> 1. 根据 `pyflink` 子模块的 Dockerfile 构建名为 `pyflink:3.14.2` 的镜像。
> 2. 构建成功后，自动调用子模块的 `start.sh`。
> 3. 从生成的镜像拉起容器或集群。
> 
> 

### 2.1 跳过本地 Python 编译

如果你已经准备好了 Python 基础镜像，有两种方式跳过本地编译：

```bash
# 方式一：从本地 tar 包导入
./deploy.sh --skip-build /tmp/cpython.tar pyflink 3.14.3

# 方式二：根据 conf.yaml 中的模板直接 docker pull
./deploy.sh --skip-build pyflink 3.14.3
```

当 `--skip-build` 没有传入 tar 包路径时，脚本会读取 `conf.yaml` 中的 `python_build.prebuilt_image_template`，
并按当前宿主机架构和目标 Python 版本组装镜像名后执行 `docker pull`。

例如在 `aarch64 + Python 3.14.3` 的情况下，会组装为：

```bash
docker pull ghcr.io/zhaolangchen2-beep/lang_env/oe2403sp1-devel-py314-gcc14-aarch64:latest
```

拉取成功后，脚本会自动将该镜像重打 tag 为内部使用的 `cpython_full:<python_version>` 或 `cpython_compact:<python_version>`。

> **注意：**
> 如果 GHCR 镜像是私有的，请先执行：
> 
> ```bash
> docker login ghcr.io
> ```

### 3. 执行测试 (TODO)

部署完成后，启动自动化测试并获取转储结果：

```bash
# 用法: ./test.sh -m <子模块名> -v <Python版本> [--redeploy]
./test.sh -m pyflink -v 3.14.2 --redeploy

```

---

## ⚠️ 新增子模块指南

若需添加新的测试模块（如 `datajuicer`），请在 `sub_modules/` 下创建新目录，并包含以下文件：

### 1. `dependency.yaml`

定义构建过程需要依赖的所有软件包名称，具体下载链接需在根目录的 `url.yaml` 中配置。

### 2. `Dockerfile` 参考模板

**基础配置与网络代理：**

```dockerfile
# 配置基础镜像
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# 接收构建参数
ARG PROXY
ARG PYTHON_VERSION

# ================== 1. 环境与网络配置 ==================
# 设置环境变量（构建时 + 运行时）
ENV http_proxy=${PROXY} \
    https_proxy=${PROXY} \
    no_proxy="localhost,127.0.0.1"

# 配置系统级代理 (针对交互式 Shell) 和 Yum SSL 容错
RUN echo "sslverify=false" >> /etc/yum.conf && \
    echo "export http_proxy=\"${PROXY}\"" >> /etc/profile.d/proxy.sh && \
    echo "export https_proxy=\"${PROXY}\"" >> /etc/profile.d/proxy.sh && \
    chmod +x /etc/profile.d/proxy.sh

```

**注入预编译 Python：**

```dockerfile
# ================== 3. 注入 Python (核心逻辑) ==================
# 从构建上下文 (主目录/tmp) 复制预编译 Python
COPY ./tmp/python_versions/${PYTHON_VERSION} /usr/local/python

# 建立软链接
RUN ln -s /usr/local/python/bin/python3 /usr/local/bin/python && \
    ln -s /usr/local/python/bin/pip3 /usr/local/bin/pip

# 配置 Python 环境变量
ENV PATH="/usr/local/python/bin:$PATH"

# 注意：如果 yum 报错，请注释掉下面这行 LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH="/usr/local/python/lib:$LD_LIBRARY_PATH"

# 配置 Pip 国内源
RUN mkdir -p ~/.pip && \
    echo "[global]" > ~/.pip/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> ~/.pip/pip.conf && \
    echo "trusted-host = pypi.tuna.tsinghua.edu.cn" >> ~/.pip/pip.conf

# ⚠️ 注意：Pip 路径在某些环境下可能失效，建议统一使用 python3 -m pip

```

### 3. `start.sh`

脚本需接受镜像名作为参数，执行从镜像拉起容器或集群的逻辑：

```bash
#!/bin/bash
IMAGE_NAME=$1
# 示例：docker run -d --name my_app $IMAGE_NAME

```

### 4. `test.sh`

*(TODO: 定义模块特有的 Benchmark 逻辑)*

---

## 📋 待办事项 (TODO)

* [ ] **扩展子模块**：集成 `datajuicer`、`cpython`、`numpy`、`pandas`。
* [ ] **测试流程**：完善 `test.sh`，支持上机一键出结果。
* [ ] **JIT 支持**：预编译 Python 支持配置 JIT 选项，并自动处理 LLVM 依赖。
* [ ] **编译模式开关**：支持关闭预编译模式，允许开发人员在子模块容器内单独编译 Python 以便调试。
