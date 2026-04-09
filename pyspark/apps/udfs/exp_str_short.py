# udfs/exp_strlen_short.py
"""实验：2 个字符串，短字符串 ~5 字节"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

_schema = StructType([
    StructField("s_out", StringType(), True),
    StructField("row_id", LongType(), True),
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    def fn(row_id, s_in, _java_ts):
        t0 = time.perf_counter_ns()
        elapsed = time.perf_counter_ns() - t0
        return (s_in, row_id, elapsed)
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    # 短字符串 ~5 bytes: "AB123"
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "CAST(id % 99999 AS STRING) AS s_in",
    )
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_out,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, s_in, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STRLEN_SHORT (~5B)", N, N

UDF_SPEC = {
    "name": "exp_str_short",
    "description": "Experiment: short strings ~5 bytes",
    "setup": setup,
}