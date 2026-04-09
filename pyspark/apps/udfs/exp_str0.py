# udfs/exp_str0.py
"""实验：10 列全数值，0 个字符串"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_schema = StructType([
    StructField("r1", DoubleType(), True),
    StructField("r2", DoubleType(), True),
    StructField("row_id", LongType(), True),
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    def fn(row_id, a, b, c, d, _java_ts):
        t0 = time.perf_counter_ns()
        r1 = a + b
        r2 = c + d
        elapsed = time.perf_counter_ns() - t0
        return (r1, r2, row_id, elapsed)
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "CAST(id * 1.1 AS DOUBLE) AS a",
        "CAST(id * 2.2 AS DOUBLE) AS b",
        "CAST(id * 3.3 AS DOUBLE) AS c",
        "CAST(id * 4.4 AS DOUBLE) AS d",
    )
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.r1, r.r2,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, a, b, c, d, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STR0 (0 strings)", N, N

UDF_SPEC = {
    "name": "exp_str0",
    "description": "Experiment: 10 fields, 0 strings",
    "setup": setup,
}