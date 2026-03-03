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
    cmake \
    make \
    g++ \
    sudo 


ENV PATH="/opt/python/bin:$PATH"
# 注意：如果 yum 报错，请注释掉下面这行 LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH="/opt/python/lib:$LD_LIBRARY_PATH"
# 验证 Python 是否可用
RUN python3 --version


# ================== 4. Flink 安装 ==================
# 复制 Spark 资源
WORKDIR /opt
ADD ./tmp/flink-2.2.0-bin-scala_2.12.tgz .
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN mv flink-2.2.0 $FLINK_HOME

# ================== 5. Python 依赖安装 ==================
# 配置 Pip 国内源
RUN mkdir -p ~/.pip && \
    echo "[global]" > ~/.pip/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> ~/.pip/pip.conf && \
    echo "trusted-host = pypi.tuna.tsinghua.edu.cn" >> ~/.pip/pip.conf

#--------pyarrow16.1.0
WORKDIR /opt
ADD ./tmp/apache-arrow-16.1.0.tar.gz .
RUN sed -i 's/_Py_IsFinalizing/Py_IsFinalizing/g' /opt/apache-arrow-16.1.0/python/pyarrow/src/arrow/python/udf.cc

WORKDIR /opt/apache-arrow-16.1.0/python
ENV CC=gcc
ENV CXX=g++
ENV PYARROW_WITH_PARQUET=1
ENV PIP_NO_VERIFY_SSL=true
RUN python3 -m pip install --no-cache-dir cython "setuptools==75.8.0" wheel "numpy<2" 

WORKDIR /opt/apache-arrow-16.1.0/cpp
RUN mkdir build && cd build && \
    cmake .. \
        -DARROW_PYTHON=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_BUILD_TYPE=Release \
        -DTHIRDPARTY_ALLOW_DOWNLOAD=ON && \
    make -j$(nproc) && \
    make install

WORKDIR /opt/apache-arrow-16.1.0/python
RUN sed -i '/def __getsegcount__/,/return 1/d' pyarrow/io.pxi && \
    sed -i '/def __getreadbuffer__/,/return self.size/d' pyarrow/io.pxi && \
    sed -i '/def __getwritebuffer__/,/return self.size/d' pyarrow/io.pxi && \
    # 再次检查：如果删除成功，grep 应该找不到这些词。如果还能找到，强制报错退出构建。
    ! grep "__getsegcount__" pyarrow/io.pxi || (echo "Error: Patch failed!" && exit 1)

RUN export ARROW_HOME=/usr/local && \
    export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH && \
    export PYARROW_WITH_PARQUET=1 && \
    python setup.py install

#-------grpcio
ENV GRPC_PYTHON_BUILD_WITH_CYTHON=1
ENV CFLAGS="-Wno-error -Wno-sign-compare"
RUN python3 -m pip install grpcio==1.65.5 grpcio-tools==1.62.1 --no-build-isolation

#-------pyflink
ENV ARROW_HOME=/usr/local
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

RUN echo "/usr/local/lib64" > /etc/ld.so.conf.d/arrow.conf && \
    ldconfig

# 2. 设置全局环境变量（对后续所有 RUN 和容器运行都生效）
ENV LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:$LD_LIBRARY_PATH
ENV ARROW_HOME=/usr/local

# 3. 切换目录验证（这次稳了）
WORKDIR /
RUN python -c "import pyarrow; print(f'PyArrow version {pyarrow.__version__} is ready!')"

RUN python3 -m pip uninstall -y cython
RUN python3 -m pip install "apache-beam==2.61.0" "apache-flink==2.2.0" --no-deps --no-build-isolation && \
    python3 -m pip install dill PyYAML ruamel.yaml "pandas<3.0" sortedcontainers objsize avro py4j typing_extensions cloudpickle crcmod httplib2 zstandard fastavro proto-plus

ENV DEFAULT_PY_PATH=/usr/local/bin/python
RUN echo "python.executable: $DEFAULT_PY_PATH" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "python.client.executable: $DEFAULT_PY_PATH" >> $FLINK_HOME/conf/flink-conf.yaml
RUN rm -rf /usr/local/python/lib/python3.14/site-packages/google/_upb
ENV no_proxy="localhost,127.0.0.1,::1"
ENV NO_PROXY="localhost,127.0.0.1,::1"

# ================== 6. 最终验证与启动 ==================
RUN java -version && \
    javac -version && \
    flink --version

# 默认启动命令（通常由 docker-compose 覆盖）
CMD ["bash"]

