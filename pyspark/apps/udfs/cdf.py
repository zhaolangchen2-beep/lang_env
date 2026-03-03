"""CDF — 标准正态 CDF（CPU 密集型标量计算）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_schema = StructType([
    StructField("input_val",   DoubleType(), True),
    StructField("cdf_result",  DoubleType(), True),
    StructField("row_id",      LongType(),   True),
    StructField("py_duration", LongType(),   True),
])


def _make_udf():
    def fn(row_id, v, _java_ts):
        if not hasattr(fn, "_cdf"):
            import math
            def _norm_cdf(x):
                a1, a2, a3, a4, a5 = (
                    0.254829592, -0.284496736, 1.421413741,
                    -1.453152027, 1.061405429,
                )
                p = 0.3275911
                sign = 1.0 if x >= 0 else -1.0
                x = abs(x) / math.sqrt(2.0)
                t = 1.0 / (1.0 + p * x)
                y = (1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)
                     * t * math.exp(-x * x))
                return 0.5 * (1.0 + sign * y)
            fn._cdf = _norm_cdf

        t0 = time.perf_counter_ns()
        result = fn._cdf(v)
        elapsed = time.perf_counter_ns() - t0
        return (v, result, row_id, elapsed)

    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count

    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        f"CAST(-4.0 + 8.0 * id / {N} AS DOUBLE) AS v",
    )

    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")

    sql = """
        SELECT r.input_val, r.cdf_result,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, v, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "CDF", N, N


UDF_SPEC = {
    "name":        "cdf",
    "description": "CDF — normal distribution CDF (CPU-intensive)",
    "setup":       setup,
}