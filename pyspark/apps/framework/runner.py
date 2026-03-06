"""
执行引擎：Spark 会话管理 + 计时 + 报告。
不包含任何数据生成或 UDF 逻辑。
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

from framework.registry import list_all, get


def parse_args():
    all_names = list_all()
    p = argparse.ArgumentParser(
        description="PySpark UDF Benchmark Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Available benchmarks:\n  " + "\n  ".join(
            f"{n:16s} {get(n).get('description','')}" for n in all_names
        ),
    )
    p.add_argument("--query", "-q", choices=all_names, default=None,
                   help="Benchmark to run")
    p.add_argument("--row_count", "-c", type=int, default=10_000_000)
    p.add_argument("--parallelism", "-a", type=int, default=4)
    p.add_argument("--person_ratio", "-r", type=int, default=10,
                   help="(NexMark) rows / persons ratio")
    p.add_argument("--list", "-l", action="store_true",
                   help="List available benchmarks and exit")
    # ── 资源配置参数 ──
    p.add_argument("--executor-memory", default="16g",
                   help="Executor memory (default: 16g)")
    p.add_argument("--executor-cores", type=int, default=4,
                   help="Cores per executor (default: 4)")
    p.add_argument("--num-executors", type=int, default=2,
                   help="Number of executors (default: 2)")
    p.add_argument("--driver-memory", default="16g",
                   help="Driver memory (default: 16g)")
    return p.parse_args()


def create_spark(args):
    jar_path = os.path.abspath(
        os.environ.get("BENCHMARK_JAR",
                       "/opt/spark/apps/spark-benchmark-udfs-java.jar")
    )
    spark = (
        SparkSession.builder
        .appName(f"UDF-Bench-{args.query}-n={args.row_count}")
        .config("spark.python.worker.reuse", "true")
        .config("spark.extraListeners", "com.example.JobTimeListener")
        .config("spark.jars", jar_path)
        .config("spark.default.parallelism", str(args.parallelism))
        .config("spark.sql.shuffle.partitions", str(args.parallelism))
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.executor.cores", str(args.executor_cores))
        .config("spark.executor.instances", str(args.num_executors))
        .config("spark.driver.memory", args.driver_memory)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.udf.registerJavaFunction("mark_start", "com.example.PreUDF", LongType())
    spark.udf.registerJavaFunction("calc_overhead", "com.example.PostUDF", LongType())
    return spark


def execute(spark, sql, tag, n_rows, expected_out=-1):
    print(f"\nRunning {tag} …")
    df = spark.sql(sql)

    t0 = time.time()
    df.write.mode("overwrite").format("noop").save()
    elapsed = time.time() - t0

    time.sleep(2)

    print("=" * 60)
    print(f"  {tag} Results")
    print("=" * 60)
    print(f"  Input rows   : {n_rows:,}")
    if expected_out >= 0:
        print(f"  Expected out : ~{expected_out:,}")
    print(f"  Wall time    : {elapsed:.2f} s")
    print(f"  Throughput   : {n_rows / elapsed:,.0f} rows/s")
    print("=" * 60)


def run():
    args = parse_args()

    if args.list:
        print("Available UDF benchmarks:")
        for name in list_all():
            spec = get(name)
            print(f"  {name:16s}  {spec.get('description', '')}")
        return

    if args.query is None:
        print("Error: --query / -q is required. Use --list to see options.")
        return

    spec = get(args.query)

    print("=" * 60)
    print(f"  UDF Benchmark: {spec['description']}")
    print("=" * 60)
    print(f"  Query       : {args.query}")
    print(f"  Rows        : {args.row_count:,}")
    print(f"  Parallelism : {args.parallelism}")
    print("-" * 60)

    spark = create_spark(args)
    try:
        sql, tag, n_rows, expected_out = spec["setup"](spark, args)
        execute(spark, sql, tag, n_rows, expected_out)
    finally:
        spark.stop()
        print("Spark session stopped.")