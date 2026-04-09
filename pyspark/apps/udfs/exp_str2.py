# udfs/exp_str2.py
"""实验：10 列，2 个字符串（输入1+输出1）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

_schema = StructType([
    StructField("s_out", StringType(), True),
    StructField("r1", DoubleType(), True),
    StructField("row_id", LongType(), True),
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    def fn(row_id, a, b, c, s_in, _java_ts):
        t0 = time.perf_counter_ns()
        r1 = a + b + c
        s_out = s_in
        elapsed = time.perf_counter_ns() - t0
        return (s_out, r1, row_id, elapsed)
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "CAST(id * 1.1 AS DOUBLE) AS a",
        "CAST(id * 2.2 AS DOUBLE) AS b",
        "CAST(id * 3.3 AS DOUBLE) AS c",
        "concat('value_', CAST(id % 1000 AS STRING)) AS s_in",
    )
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_out, r.r1,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, a, b, c, s_in, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STR2 (2 strings)", N, N

UDF_SPEC = {
    "name": "exp_str2",
    "description": "Experiment: 10 fields, 2 strings",
    "setup": setup,
}