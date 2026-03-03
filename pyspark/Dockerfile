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

# ================== 2. 系统依赖安装 (YUM) ==================
RUN yum clean all && yum makecache && \
    yum install -y \
    openssl-libs \
    zlib \
    bzip2-libs \
    xz-libs \
    libffi \
    ca-certificates \
    java-17-openjdk-devel \
    curl \
    wget \
    openssh-server \
    procps-ng \
    net-tools \
    bind-utils \
    which \
    tar \
    gzip \
    sudo && \
    yum clean all && \
    rm -rf /var/cache/yum


# 验证 Python 是否可用
RUN python3 --version

# ================== 4. Spark 安装 ==================
# 复制 Spark 资源
COPY ./tmp/spark-4.1.0-bin-hadoop3.tgz /tmp/
COPY ./tmp/pyspark-4.1.0.tar.gz /tmp/

# 安装 Spark
RUN tar -xzf /tmp/spark-4.1.0-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-4.1.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-4.1.0-bin-hadoop3.tgz

# 设置 Java 和 Spark 环境变量
ENV JAVA_HOME=/usr/lib/jvm/jre-17-openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# 修正：添加当前目录到 PYTHONPATH 方便调试
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# ================== 5. Python 依赖安装 ==================
# 配置 Pip 国内源
RUN mkdir -p ~/.pip && \
    echo "[global]" > ~/.pip/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> ~/.pip/pip.conf && \
    echo "trusted-host = pypi.tuna.tsinghua.edu.cn" >> ~/.pip/pip.conf

# 安装 PySpark 和业务库
# 使用 --no-cache-dir 减小镜像体积
RUN PIP_NO_VERIFY_SSL=true \
    python3 -m pip install --no-cache-dir /tmp/pyspark-4.1.0.tar.gz && \
    python3 -m pip install --no-cache-dir pandas pyarrow

# ================== 6. 最终验证与启动 ==================
RUN java -version && \
    javac -version && \
    spark-submit --version && \
    python3 -c "import pyspark; print('PySpark installed:', pyspark.__version__)"

CMD ["bash"]
