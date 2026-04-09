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

# 验证 Python 是否可用
RUN python3 --version

COPY ./tmp/cinderx.tgz /tmp/

RUN tar -xzf /tmp/cinderx.tgz -C /opt/ && \
    rm /tmp/cinderx.tgz

# 配置 Pip 国内源
RUN mkdir -p ~/.pip && \
    echo "[global]" > ~/.pip/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> ~/.pip/pip.conf && \
    echo "trusted-host = pypi.tuna.tsinghua.edu.cn" >> ~/.pip/pip.conf 
# 安装 PySpark 和业务库
# 使用 --no-cache-dir 减小镜像体积
RUN PIP_NO_VERIFY_SSL=true \
    python3 -m pip install pyperformance==1.13.0
# ================== 6. 最终验证与启动 ==================
# 使用 sed 动态修改 pyperformance 源码中的 venv 创建逻辑
RUN VENV_SCRIPT=$(python3 -c "import pyperformance; print(pyperformance.__path__[0] + '/_venv.py')") && \
    # 修改位置 1: 在 withpip=True 的分支添加 --system-site-packages
    sed -i 's/args = \["-m", "venv", root\]/args = \["-m", "venv", "--system-site-packages", root\]/g' "$VENV_SCRIPT" && \
    # 修改位置 2: 在 else 分支添加 --system-site-packages
    sed -i 's/args = \["-m", "venv", "--without-pip", root\]/args = \["-m", "venv", "--without-pip", "--system-site-packages", root\]/g' "$VENV_SCRIPT" && \
    # 验证修改是否成功 (可选)
    grep "system-site-packages" "$VENV_SCRIPT"  

    # 1. 显式设置所有编译相关的路径，不依赖 profile

ENV PATH="/opt/gcc-14/bin:/usr/local/bin:${PATH}" \
    CC="/opt/gcc-14/bin/gcc" \
    CXX="/opt/gcc-14/bin/g++"

RUN cd /opt/cinderx/ && \
    rm -rf scratch/ build/ *.egg-info CMakeCache.txt && \
    #CINDERX_ENABLE_PGO=1 \
    #CINDERX_ENABLE_LTO=1 \
    ENABLE_STATIC_PYTHON=0 \
    python3 -m pip install . 
CMD ["bash"]
