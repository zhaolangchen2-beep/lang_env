"""TPC-H Q6 — Forecasting Revenue Change（最简单）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_schema = StructType([
    StructField("revenue_part", DoubleType(), True),
    StructField("row_id",       LongType(),  True),
    StructField("py_duration",  LongType(),  True),
])


def _gen_lineitem(spark, n_rows, parallelism):
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    def fn(row_id, l_extendedprice, l_discount, l_quantity,
           l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        # 1994-01-01=8766, 1995-01-01=9131
        if (8766 <= l_shipdate_days < 9131
                and 0.05 <= l_discount <= 0.07
                and l_quantity < 24):
            result = l_extendedprice * l_discount
        else:
            result = 0.0
        elapsed = time.perf_counter_ns() - t0
        return (result, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    li = _gen_lineitem(spark, N, args.parallelism)

    spark.udf.register("process", _make_udf())
    li.createOrReplaceTempView("src")

    sql = """
        SELECT r.revenue_part,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount, l_quantity,
                l_shipdate_days, mark_start(l_rowid)
            ) AS r
            FROM src
        ) t
    """
    return sql, "TPC-H Q6", N, N


UDF_SPEC = {
    "name":        "tpch_q6",
    "description": "TPC-H Q6 — Forecasting Revenue Change (simplest)",
    "setup":       setup,
}