# udfs/tpch_q15.py
"""TPC-H Q15 — Top Supplier
原始 SQL：创建 revenue 视图，找季度内 revenue 最高的供应商。
UDF：每行做日期过滤 + revenue 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

_schema = StructType([
    StructField("l_suppkey",     LongType(),   True),
    StructField("revenue",       DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    n_supp = max(n_rows // 600, 100)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"CAST(id % {n_supp} AS BIGINT) AS l_suppkey",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    # 1996-01-01 = 9497, 1996-04-01 = 9588
    _start = 9497
    _end = 9588

    def fn(row_id, l_suppkey, l_extendedprice, l_discount,
           l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if _start <= l_shipdate_days < _end:
            revenue = l_extendedprice * (1.0 - l_discount)
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (l_suppkey, revenue, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.l_suppkey, r.revenue, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_suppkey, l_extendedprice, l_discount,
                l_shipdate_days, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q15", N, N

UDF_SPEC = {
    "name": "tpch_q15",
    "description": "TPC-H Q15 — Top Supplier",
    "setup": setup,
}