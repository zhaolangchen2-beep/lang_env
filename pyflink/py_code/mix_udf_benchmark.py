import argparse
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes, StreamTableEnvironment, EnvironmentSettings, TableFunction, FunctionContext
from pyflink.table.udf import udtf


class PyProcess(TableFunction):
    def __init__(self):
        self.count = None
        self.sum_overhead = None

    def open(self, function_context: FunctionContext):
        self.count = 0
        self.sum_overhead = 0

    def eval(self, price: float, java_start_time: int):
        t_start = time.perf_counter_ns()

        # Simulate some Python processing
        price = price + 1

        t_end = time.perf_counter_ns()
        py_duration = t_end - t_start

        # 性能统计逻辑
        self.count += 1
        self.sum_overhead += py_duration

        # 每 1M 条数据打印一次日志
        if self.count % 1000000 == 0:
            avg_overhead = self.sum_overhead / self.count
            print(
                f"[Python UDF]: Count={self.count}, Average Overhead={avg_overhead}, Sum Overhead={self.sum_overhead}")

        # Yield a Row containing the three output fields
        yield price, java_start_time, py_duration


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='PyFlink NexMark Benchmark')
    parser.add_argument('--pandas_switch', '-p', action='store_true',
                        help='Use Pandas UDF or not')
    parser.add_argument('--idle_switch', '-i', action='store_true',
                        help='Run UDF in idle or not')
    parser.add_argument('--profiling', '-r', action='store_true',
                        help='Enable profiling or not')
    parser.add_argument('--row_count', '-c', type=int, default=10000000,
                        help='Total number of rows to generate')
    parser.add_argument('--rows_per_second', '-s', type=int, default=200000000,
                        help='The speed of generating rows')
    parser.add_argument('--parallelism', '-a', type=int, default=1,
                        help='Parallelism of the job')
    parser.add_argument('--batch_size', '-b', type=int, default=10000,
                        help='Batch size of arrow')
    parser.add_argument('--bundle_time', '-t', type=int, default=1000,
                        help='Bundle time in milliseconds')
    parser.add_argument('--execution_mode', '-e', type=str, default='process',
                        help='Python execution mode')
    return parser.parse_args()


def run_benchmark():
    args = parse_args()

    # 1. 初始化环境
    # 增加并行度以利用多核CPU（根据你的电脑核心数调整，比如 4）
    gateway = get_gateway()
    string_class = gateway.jvm.String
    string_array = gateway.new_array(string_class, 0)
    stream_env = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    j_stream_execution_environment = stream_env.createRemoteEnvironment(
        "localhost",
        8081,
        string_array
    )
    # env = StreamExecutionEnvironment(j_stream_execution_environment)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    # 生成t_env
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    configuration = t_env.get_config().get_configuration()
    configuration.set_string("python.executable", "/usr/local/bin/python")
    configuration.set_string("python.client.executable", "/usr/local/bin/python")
    # configuration.set_string("taskmanager.env.PYTHONHOME", "/home/l30044810/PyFlinkDemo/pynexmark/.venv")
    # configuration.set_string("taskmanager.env.PYTHONPATH", "/home/l30044810/PyFlinkDemo/pynexmark/.venv/lib/python3.9/site-packages")
    configuration.set_string("pipeline.name", f"PyNexmark-Pandas-UDF-{time.time()}")
    configuration.set_string("taskmanager.numberOfTaskSlots", f"{args.parallelism}")
    configuration.set_boolean("python.profiling.enabled", args.profiling)

    # 优化 Pandas UDF 的配置 (可选，增大 Batch 大小可以进一步提升吞吐)
    # t_env.add_python_archive("venv.zip")
    # t_env.get_config().set_python_executable("venv.zip/venv/bin/python")
    t_env.get_config().set("parallelism.default", f"{args.parallelism}")
    t_env.get_config().set("python.fn-execution.arrow.batch.size", f"{args.batch_size}")
    t_env.get_config().set("python.fn-execution.bundle.size", f"{args.batch_size}")
    t_env.get_config().set("python.fn-execution.bundle.time", f"{args.bundle_time}")
    t_env.get_config().set("python.execution-mode", args.execution_mode)
    t_env.get_config().set("table.local-time-zone", "Asia/Shanghai")
    t_env.get_config().set("python.profiling.output.dir", "/tmp/flink_profiles")
    t_env.get_config().set("pipeline.jars", "file:///opt/py_code/FlinkDemo-1.0-SNAPSHOT.jar")

    # 数据量：100 万条
    row_count = args.row_count

    # 2. 创建 DataGen 源表 (在 TaskManager 端极速生成数据)
    source_ddl = f"""
        CREATE TABLE source_table (
            id INT,
            price DOUBLE,
            quantity INT
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '{row_count}',
            'rows-per-second' = '{args.rows_per_second}',
            'fields.id.kind' = 'sequence',
            'fields.id.start' = '1',
            'fields.id.end' = '{row_count}',
            'fields.price.min' = '1.0',
            'fields.price.max' = '100.0',
            'fields.quantity.min' = '1',
            'fields.quantity.max' = '10'
        )
    """
    t_env.execute_sql(source_ddl)

    # 3. 创建 BlackHole 结果表 (避免打印 IO 影响测速)
    sink_ddl = """
               CREATE TABLE sink_table
               (
                   id             BIGINT,
                   price DOUBLE,
                   totalRoundTrip BIGINT,
                   pyDurationNs   BIGINT,
                   overhead       BIGINT
               ) WITH (
                     'connector' = 'blackhole'
                     )
               """
    t_env.execute_sql(sink_ddl)

    # ==========================================
    # 定义测试逻辑：加上一些数学运算模拟真实负载
    # ==========================================

    pandas_sw = args.pandas_switch
    idle_sw = args.idle_switch

    # 场景 A: 普通 Python UDF (逐行调用，序列化开销大)
    @udtf(result_types=[
        DataTypes.DOUBLE(),
        DataTypes.BIGINT(),
        DataTypes.BIGINT()
    ])
    def timed_python_udf(price: float, java_start_time: int):
        t_start = time.perf_counter_ns()

        # Simulate some Python processing
        price = price + 1

        t_end = time.perf_counter_ns()
        py_duration = t_end - t_start

        # Yield a Row containing the three output fields
        yield price, java_start_time, py_duration

    # 场景 B: Pandas UDF (向量化调用，极低开销)
    # @udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
    # def pandas_calc(price: pd.Series, quantity: pd.Series) -> pd.Series:
    #     # 使用 numpy 进行向量化运算
    #     # np.log 是 C 语言实现的底层数组运算
    #     if idle_sw:
    #         return price
    #     else:
    #         return (price * quantity) + np.log(price + 1)

    # 注册函数
    t_env.create_java_temporary_function("MarkStart", "com.huawei.PreUDF")
    t_env.create_java_temporary_function("CalcOverhead", "com.huawei.PostUDF")
    # t_env.create_java_temporary_function("PyProcess", "com.huawei.TimedJavaUDF")

    t_env.create_temporary_function("PyProcess", udtf(PyProcess(), result_types=[DataTypes.DOUBLE(), DataTypes.BIGINT(),
                                                                                 DataTypes.BIGINT()]))
    # t_env.create_temporary_function("PyProcess", timed_python_udf)

    print(f"开始生成并处理 {row_count} 条数据...")
    print("-" * 50)
    print(f"pandas开关： {pandas_sw}, idle开关：{idle_sw}, profiling开关：{args.profiling}")

    start_time = time.time()
    statement_set = t_env.create_statement_set()

    # -------------------------------------------------------
    # 测试 1: 普通 Python UDF
    # -------------------------------------------------------
    if not pandas_sw:
        print("正在运行: [普通 Python UDF] ...")
        #     statement_set.add_insert_sql("""
        # INSERT INTO sink_table
        # SELECT
        #     id,
        #     final_result.row_data.price,
        #     final_result.row_data.totalRoundTrip,
        #     final_result.row_data.pyDurationNs,
        #     final_result.row_data.overhead
        # FROM (
        #     SELECT
        #         id,
        #         CalcOverhead(
        #             T.processed_data,
        #             T.java_start_time,
        #             T.py_duration
        #         ) AS row_data
        #     FROM source_table,
        #     -- 使用 LATERAL TABLE 展开 Python UDF 返回的复杂结构
        #     LATERAL TABLE(
        #         PyProcess(
        #             MarkStart(source_table.price)
        #         )
        #     ) AS T(processed_data, java_start_time, py_duration)
        # ) AS final_result
        #                       """)

        statement_set.add_insert_sql("""
                                     INSERT INTO sink_table
                                     SELECT final_result.id,
                                            final_result.row_data.price,
                                            final_result.row_data.totalRoundTrip,
                                            final_result.row_data.pyDurationNs,
                                            final_result.row_data.overhead
                                     FROM (WITH Step1_MarkStart AS (
                                         -- Step 1: Apply MarkStart Java Scalar UDF
                                         -- MarkStart returns a ROW (original_content, java_start_time)
                                         SELECT T.id,
                                                MarkStart(T.price) AS marked_data_row -- The UDF returns a ROW
                                         FROM source_table AS T),
                                                Step2_PyProcess AS (
                                                    -- Step 2: Apply PyProcess PyFlink Table Function (UDTF)
                                                    -- PyProcess takes (original_content, java_start_time) and returns (processed_data, py_start_time, py_duration)
                                                    SELECT s1.id,
                                                           TPY.price,           -- Field from PyProcess UDTF output
                                                           TPY.java_start_time, -- Field from PyProcess UDTF output
                                                           TPY.py_duration      -- Field from PyProcess UDTF output
                                                    FROM Step1_MarkStart AS s1,
                                                         -- Use LATERAL TABLE for UDTF. Unpack the ROW from marked_data_row for PyProcess's arguments.
                                                         -- The AS TPY(...) part matches the field names defined in PyProcess's result_types.
                                                         LATERAL TABLE (PyProcess(s1.marked_data_row.price, s1.marked_data_row.javaStartTime))
                                              AS TPY(price, java_start_time, py_duration)
                                         )
                                     -- Step 3: Apply CalcOverhead Java Scalar UDF
                                     -- CalcOverhead returns a ROW (final_data, total_duration, overhead_ms, status_msg)
                                     SELECT s2.id,
                                            CalcOverhead(
                                                    s2.price,
                                                    s2.java_start_time, -- From MarkStart
                                                    s2.py_duration -- From PyProcess
                                            ) AS row_data -- The UDF returns a ROW
                                     FROM Step2_PyProcess AS s2 ) AS final_result
                                     """)

    # -------------------------------------------------------
    # 测试 2: Pandas UDF
    # -------------------------------------------------------
    else:
        print("正在运行: [Pandas UDF] ...")
        statement_set.add_insert_sql("""
                                     INSERT INTO sink_table
                                     SELECT id, pandas_calc(price, quantity)
                                     FROM source_table
                                     """)

    table_result = statement_set.execute()

    # 等待作业完成
    try:
        # 尝试获取job client
        job_client = table_result.get_job_client()
        if job_client:
            print(f"Job ID: {job_client.get_job_id()}")
            # 等待作业完成
            job_client.get_job_execution_result().result()
        else:
            # 如果没有job client，简单等待
            time.sleep(10)
    except Exception as e:
        print(f"Job execution: {e}")
        # 流作业可能不会自然结束，等待一段时间后继续
        time.sleep(5)

    duration_pandas = time.time() - start_time
    print(f"耗时: {duration_pandas:.2f} 秒")
    print(f"吞吐: {row_count / duration_pandas:.0f} 条/秒")


if __name__ == '__main__':
    run_benchmark()
