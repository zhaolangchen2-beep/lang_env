# pyspark_benchmark.py
import argparse
import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, pandas_udf, struct, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    IntegerType,
)
import os
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser(description='PySpark NexMark Benchmark')
    parser.add_argument('--pandas_switch', '-p', action='store_true',
                        help='Use Pandas UDF or not')
    parser.add_argument('--idle_switch', '-i', action='store_true',
                        help='Run UDF in idle mode (no computation)')
    parser.add_argument('--row_count', '-c', type=int, default=10_000_000,
                        help='Total number of rows to generate')
    parser.add_argument('--parallelism', '-a', type=int, default=4,
                        help='Spark default parallelism')
    parser.add_argument('--batch_size', '-b', type=int, default=10000,
                        help='Pandas UDF batch size (for Arrow)')
    parser.add_argument('--py_switch', '-m', action='store_true',
                        help='Python udf or java udf')
    return parser.parse_args()

# Regular Python UDF (row-at-a-time)
def timed_python_udf(price: float, java_start_time: int):
    """Regular Python UDF that processes a single row at a time"""
    t0 = time.perf_counter_ns()
    price = price + 1
    t1 = time.perf_counter_ns()
    return price, java_start_time, t1 - t0

result_schema = StructType([
        StructField("price", DoubleType(), True),
        StructField("java_start_time", LongType(), True),
        StructField("py_duration", LongType(), True)
    ])

@pandas_udf(result_schema)
def timed_pandas_udf(price_series: pd.Series, java_start_time_series: pd.Series) -> pd.DataFrame:
    """
    Pandas UDF: 一次处理一个 Batch（由 spark.sql.execution.arrow.maxRecordsPerBatch 定义）
    """
    t0 = time.perf_counter_ns()

    # 向量化加法：这在底层调用的是 C/C++ 或 SIMD 指令
    new_price = price_series + 1

    t1 = time.perf_counter_ns()

    # 计算整个 Batch 的逻辑总耗时
    batch_duration = t1 - t0
    # 平摊到每一行
    per_row_duration = batch_duration // len(price_series)

    return pd.DataFrame({
        "price": new_price,
        "java_start_time": java_start_time_series,
        "py_duration": [per_row_duration] * len(price_series)
    })

def timed_rdd_udf(iterator):
    for row in iterator:
        # 模拟原有 UDF 的计算逻辑
        t0 = time.perf_counter_ns()

        new_price = row.price + 1
        # 可以在这里加入更复杂的计算来观察 Profiler 结果

        t1 = time.perf_counter_ns()
        py_duration = t1 - t0

        # 返回一个新的 Row 或 Tuple
        yield Row(
            id=row.id,
            price=new_price,
            py_duration=py_duration,
            java_start_time=0  # RDD 模式下很难直接穿插调用 Java UDF，设为0
        )

def main():
    args = parse_args()
    print(f"Starting PySpark Benchmark with configuration:")
    print(f"- Pandas UDF mode: {'ENABLED' if args.pandas_switch else 'DISABLED'}")
    print(f"- Idle mode: {'ENABLED' if args.idle_switch else 'DISABLED'}")
    print(f"- Py mode: {'ENABLED' if args.py_switch else 'DISABLED'}")
    print(f"- Row count: {args.row_count:,}")
    print(f"- Parallelism: {args.parallelism}")
    print(f"- Batch size: {args.batch_size}")
    print("-" * 60)

    jar_path = os.path.abspath("/opt/spark/apps/spark-benchmark-udfs-java.jar")
    # Initialize Spark Session
    if args.pandas_switch:
        arrow_enable = "true"
    else:
        arrow_enable = "false"
    #     .config("spark.python.profile", "true") \
    spark = SparkSession.builder \
        .appName(f"PySpark-Benchmark-Pandas-{args.pandas_switch}") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", f"{arrow_enable}") \
        .config("spark.extraListeners", "com.example.JobTimeListener") \
        .config("spark.jars", jar_path) \
        .config("spark.default.parallelism", str(args.parallelism)) \
        .config("spark.sql.shuffle.partitions", str(args.parallelism)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Generate test data
    print(f"Generating {args.row_count:,} rows of test data...")

    # Create DataFrame with id, price, and quantity columns
    df = spark.range(0, args.row_count, numPartitions=args.parallelism).toDF("id")
    df = df.withColumn("price", (col("id") % 100 + 1).cast("double"))
    df = df.withColumn("quantity", (col("id") % 10 + 1).cast("int"))

    if args.pandas_switch:
        spark.udf.register("process", timed_pandas_udf)
    elif args.py_switch:
        py_udf = udf(timed_python_udf, result_schema)
        spark.udf.register("process", py_udf)
    else:
        spark.udf.registerJavaFunction("process", "com.example.TimedJavaUDF", result_schema)
    spark.udf.registerJavaFunction("mark_start", "com.example.PreUDF", LongType())
    spark.udf.registerJavaFunction("calc_overhead", "com.example.PostUDF", LongType())
    
    # if args.py_switch:
    #     # For Python UDF mode, convert to RDD early to enable profiling
    #     print("Using RDD API for Python UDF processing...")
    #
    #     # First, apply mark_start using DataFrame API to get java_start_time
    #     df_with_java_start = df.withColumn("java_start_time", expr("mark_start(price)"))
    #
    #     # Convert to RDD for Python UDF processing
    #     rdd = df_with_java_start.rdd
    #
    #     def process_row_with_timing(row):
    #         """Process a single row with Python UDF logic and timing"""
    #         t0 = time.perf_counter_ns()
    #         new_price = row.price + 1  # Same logic as timed_python_udf
    #         t1 = time.perf_counter_ns()
    #         py_duration = t1 - t0
    #
    #         # Return as Row with all required fields
    #         return Row(
    #             id=row.id,
    #             price=new_price,
    #             java_start_time=row.java_start_time,
    #             py_duration=py_duration
    #         )
    #
    #     # Apply Python UDF logic via RDD map
    #     processed_rdd = rdd.map(process_row_with_timing)
    #
    #     # Convert back to DataFrame with proper schema
    #     processed_schema = StructType([
    #         StructField("id", LongType(), True),
    #         StructField("price", DoubleType(), True),
    #         StructField("java_start_time", LongType(), True),
    #         StructField("py_duration", LongType(), True)
    #     ])
    #
    #     processed_df = spark.createDataFrame(processed_rdd, processed_schema)
    #
    #     # Apply calc_overhead using DataFrame API
    #     final_df = processed_df.withColumn(
    #         "overhead_ms",
    #         expr("calc_overhead(price, java_start_time, py_duration)")
    #     ).select("id", "price", "py_duration", "overhead_ms")

    # else:
    df.createOrReplaceTempView("source_table")
    # Register Java UDFs - these must be packaged in a JAR and provided via --jars
    print("Registering UDFs...")

    benchmark_sql = """
    SELECT 
        id,
        r.price,
        r.py_duration,
        calc_overhead(r.price, r.java_start_time, r.py_duration) as overhead_ms
    FROM (
        SELECT 
            id,
            process(price, mark_start(price)) as r
        FROM source_table
    )
    """
    # Apply the UDF

    final_df = spark.sql(benchmark_sql)

    # Trigger execution - this is equivalent to the BlackHole sink in Flink
    start_exec = time.time()
    final_df.write.mode("overwrite").format("noop").save()
    exec_time = time.time() - start_exec
    time.sleep(2)
    # sc = spark.sparkContext
    # sc.show_profiles()

    # Print results
    print("-" * 60)
    print(f"Benchmark completed successfully!")
    print(f"Total rows processed: {args.row_count:,}")
    print(f"Execution time: {exec_time:.2f} seconds")
    print(f"Throughput: {args.row_count / exec_time:,.0f} rows/second")
    print("-" * 60)
    # Clean up

    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    main()
