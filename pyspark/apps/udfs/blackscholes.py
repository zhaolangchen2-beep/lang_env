# udfs/blackscholes.py
"""Black-Scholes 期权定价（CPU 重计算）"""

import time, math
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_schema = StructType([
    StructField("call_price",  DoubleType(), True),
    StructField("row_id",      LongType(),   True),
    StructField("py_duration", LongType(),   True),
])


def _gen_options(spark, n_rows, parallelism):
    """每个 case 自行决定要什么数据、什么列名。"""
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS row_id",
        "CAST(80 + (id % 40) AS DOUBLE) AS spot",
        "CAST(100.0 AS DOUBLE) AS strike",
        "CAST(0.5 + (id % 20) * 0.05 AS DOUBLE) AS time_to_exp",
        "CAST(0.05 AS DOUBLE) AS risk_free",
        "CAST(0.15 + (id % 10) * 0.03 AS DOUBLE) AS volatility",
    )


def _make_udf():
    def fn(row_id, S, K, T, r, sigma, _java_ts):
        t0 = time.perf_counter_ns()
        d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        # Abramowitz & Stegun CDF
        def ncdf(x):
            a1,a2,a3,a4,a5 = 0.254829592,-0.284496736,1.421413741,-1.453152027,1.061405429
            p = 0.3275911; s = 1.0 if x>=0 else -1.0; x=abs(x)/math.sqrt(2.0)
            t=1.0/(1.0+p*x)
            y=1.0-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-x*x)
            return 0.5*(1.0+s*y)
        call = S*ncdf(d1) - K*math.exp(-r*T)*ncdf(d2)
        elapsed = time.perf_counter_ns() - t0
        return (call, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_options(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.call_price,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, spot, strike, time_to_exp,
                           risk_free, volatility, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "Black-Scholes", N, N


UDF_SPEC = {
    "name":        "blackscholes",
    "description": "Black-Scholes option pricing (heavy math)",
    "setup":       setup,
}