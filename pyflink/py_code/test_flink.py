from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
import os

# 确保 Java 也能找到我们的 Python 环境
# (在某些环境下，Flink 可能会尝试调用系统默认 python，所以强制指定一下)
# os.environ['PYFLINK_PYTHON'] = '/opt/.venv/bin/python' 

def hello_world():
    # 1. 创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("开始准备数据...")

    # 2. 创建一个简单的数据流 (测试 Python <-> Java 的序列化)
    ds = env.from_collection(
        collection=[
            (1, 'Hello'),
            (2, 'PyFlink'),
            (3, 'On'),
            (4, 'Python 3.14'),
            (5, 'Success!')
        ],
        type_info=Types.ROW([Types.INT(), Types.STRING()])
    )

    # 3. 打印结果 (标准输出)
    ds.print()

    # 4. 执行任务
    print("提交任务中...")
    env.execute("Test Python 3.14 Job")

if __name__ == '__main__':
    hello_world()
