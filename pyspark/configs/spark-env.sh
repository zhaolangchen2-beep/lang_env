#!/bin/bash

# 关键配置：使用容器名作为主机名
export SPARK_MASTER_HOST=pyspark-spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker配置
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_PORT=8888
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=16g

# 内存配置
export SPARK_DAEMON_MEMORY=16g
export SPARK_DRIVER_MEMORY=16g
export SPARK_EXECUTOR_MEMORY=16g

# 目录配置
export SPARK_LOCAL_DIRS=/tmp/spark
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_PID_DIR=/tmp

# Java配置
# openEuler中查找Java安装路径
if [ -d "/usr/lib/jvm/java-11-openjdk" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
elif [ -d "/usr/lib/jvm/java-1.8.0" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0
else
    # 尝试查找java命令位置
    JAVA_PATH=$(which java 2>/dev/null)
    if [ -n "$JAVA_PATH" ]; then
        export JAVA_HOME=$(dirname $(dirname $JAVA_PATH))
    else
        export JAVA_HOME=/usr
    fi
fi

# Python配置
# export PYSPARK_PYTHON=/opt/spark/apps/profiler_wrapper.sh
# export PYSPARK_PYTHON=python3
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
# --- Python 3.14 JIT 相关开关 ---
# 开启 JIT (对应你编译时的 yes-off)
export PYTHON_JIT=1

# 设置 JIT 触发阈值
# 在压测环境下设为 100，确保 UDF 运行初期就能完成编译
export PYTHON_JIT_THRESHOLD=1


# Docker网络配置 - 使用容器名，Web UI链接使用宿主机端口
export SPARK_PUBLIC_DNS=localhost

