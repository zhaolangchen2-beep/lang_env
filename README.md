lang_env自动化环境部署与测试框架
lang_env是一个专为Python生态项目设计的自动化Docker环境部署与基准测试框架。

🚀 核心特性
架构自适应 (Arch-Aware)： 自动识别 x86_64 / aarch64 架构，精准匹配华为云或官方镜像源。

容器化预编译： Python编译在隔离容器中完成并保存产物在主机，各模块部署镜像时直接取用产物，保证测试一致性，并节省编译时间

参数化优化： 通过 conf.yaml 注入Python版本、编译参数、代理等配置

解耦设计： 主框架只负责“基础设施”，具体的业务逻辑封装在子模块中。

📂 项目目录结构
Plaintext
lang_test/
├── conf.yaml          # 全局配置中心（代理、Docker版本、编译参数）
├── url.yaml           # 资源索引（Python源码、基础镜像、组件包链接）
├── tmp/               # 资源池（存放下载包及预编译的 Python 产物）
├── deploy.sh          # 【主入口】环境部署脚本
├── test.sh            # 【主入口】测试驱动脚本 
├── out/               # 测试结果转储（日志、性能数据、SVG图表）
└── sub_modules/       # 子模块
    ├── dependency.yaml # 模块私有依赖定义
    ├── Dockerfile      # 镜像构建文件
    ├── start.sh        # 集群/容器拉起逻辑
    └── test.sh         # 模块特有的 benchmark 逻辑

🛠️ 快速开始
1. 配置全局变量
在 conf.yaml 中设置你的代理和目标 Python 版本：

global:
  proxy: "http://90.91.56.202:3128"                     # 为主机执行时配置代理，并且构建镜像时作为参数配置
  docker_min_version: "26.1.3"                          # 最低docker版本，如果不满足则会配置yum源并尝试安装该版本
  base_os_image: "openeuler-24.03-lts-sp1:latest"       # 基础OS镜像

python_build:
  # 允许测试的 Python 版本列表
  supported_versions:
      - "3.14.2"
  # 预编译参数，用于 deploy.sh 生成通用 Python
  cflags: "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"
  ldflags: ""
  configure_args: "--enable-optimizations --with-lto"
  install_dir: "/tmp/lang_test/python_dist"

2. 一键部署
执行部署脚本

Bash
# 用法: ./deploy.sh <子模块名> <Python版本>
./deploy.sh pyflink 3.14.2

会根据子模块的Dockerfile构建名为 pyflink:3.14.2的镜像
构建成功后会调用子模块的start.sh，从镜像拉起容器或集群

3. 执行测试
 (TODO)
部署，启动自动化测试并获取转储结果：

Bash
# 用法: ./test.sh -m <子模块名> -v <Python版本> [--redeploy]
./test.sh -m pyflink -v 3.14.2 --redeploy


⚠️ 新增子模块
 sub_modules/           # 子模块
    ├── dependency.yaml # 模块私有依赖定义
    ├── Dockerfile      # 镜像构建文件
    ├── start.sh        # 集群/容器拉起逻辑
    └── test.sh         # 模块特有的 benchmark 逻辑

1. dependency.yaml
构建过程需要依赖的所有软件包名称写在 dependency.yaml中，并在url.yaml中配置获取链接

2. Dockerfile
—— 使用参数配置基础镜像和代理参考：
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

—— 使用预编译的Python参考：
# ================== 3. 注入 Python (核心逻辑) ==================
# 从构建上下文 (主目录/tmp) 复制预编译 Python
COPY ./tmp/python_versions/${PYTHON_VERSION} /usr/local/python
RUN ln -s /usr/local/python/bin/python3 /usr/local/bin/python && \
    ln -s /usr/local/python/bin/pip3 /usr/local/bin/pip

# 配置 Python 环境变量
ENV PATH="/usr/local/python/bin:$PATH"
# 注意：如果 yum 报错，请注释掉下面这行 LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH="/usr/local/python/lib:$LD_LIBRARY_PATH"

Pip 路径失效问题： 统一使用 python3 -m pip。

—— 配置pip国际源
# 配置 Pip 国内源
RUN mkdir -p ~/.pip && \
    echo "[global]" > ~/.pip/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> ~/.pip/pip.conf && \
    echo "trusted-host = pypi.tuna.tsinghua.edu.cn" >> ~/.pip/pip.conf

3. start.sh 
脚本需要接受镜像名作为参数，按照该模块需要，执行从镜像拉起容器或集群：
./start.sh pyspark:3.14.2

4. test.sh
//TODO


     
📋 待办事项 (TODO)
1. 子模块：datajuicer\cpython\numpy\pandas

2. 测试模块：支持上机一键出结果

3. 预编译Python JIT依赖LLVM：支持conf配置JIT时获取LLVM

4. 支持预编译模式开关：关掉时每个子模块单独编译Python，方便开发人员自己在容器编译调试

5. 确认yum安装的软件能否分享。