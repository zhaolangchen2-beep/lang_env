"""ADD — 一次加法（纯框架开销基线）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType

# ── schema ────────────────────────────────────────────────────
_schema = StructType([
    StructField("result",      LongType(), True),
    StructField("row_id",      LongType(), True),
    StructField("py_duration", LongType(), True),
])

# ── UDF ───────────────────────────────────────────────────────
def _make_udf():
    def fn(row_id, a, b, _java_ts):
        t0 = time.perf_counter_ns()
        result = a + b
        elapsed = time.perf_counter_ns() - t0
        return (result, row_id, elapsed)
    return udf(fn, _schema)

# ── data generator + setup ────────────────────────────────────
def setup(spark, args):
    N = args.row_count

    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "CAST(id AS BIGINT) AS a",
        "CAST(id * 7 + 3 AS BIGINT) AS b",
    )

    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")

    sql = """
        SELECT r.result,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, a, b, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "ADD", N, N


UDF_SPEC = {
    "name":        "add",
    "description": "ADD — simple addition (baseline overhead)",
    "setup":       setup,
}