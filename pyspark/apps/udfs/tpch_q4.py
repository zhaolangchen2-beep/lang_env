# udfs/tpch_q4.py
"""TPC-H Q4 — Order Priority Checking
原始 SQL：统计指定季度内有延迟交付行的订单数，按优先级分组。
UDF：每行做日期范围过滤 + commitdate vs receiptdate 比较。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("o_orderpriority", StringType(), True),
    StructField("is_late",         LongType(),   True),
    StructField("in_range",        LongType(),   True),
    StructField("row_id",          LongType(),   True),
    StructField("py_duration",     LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    prios = sql_array(ORDER_PRIORITIES)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({prios}, CAST((id % {len(ORDER_PRIORITIES)}) + 1 AS INT)) AS o_orderpriority",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id + 30) % 2556 AS INT)), DATE '1970-01-01') AS l_commitdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id + 60) % 2556 AS INT)), DATE '1970-01-01') AS l_receiptdate_days",
    )


def _make_udf():
    # 1993-07-01 = 8582, 1993-10-01 = 8674
    _start = 8582
    _end = 8674

    def fn(row_id, o_orderpriority, o_orderdate_days,
           l_commitdate_days, l_receiptdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        in_range = 1 if _start <= o_orderdate_days < _end else 0
        is_late = 1 if l_commitdate_days < l_receiptdate_days else 0
        elapsed = time.perf_counter_ns() - t0
        return (o_orderpriority, is_late, in_range, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.o_orderpriority, r.is_late, r.in_range,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, o_orderpriority, o_orderdate_days,
                l_commitdate_days, l_receiptdate_days, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q4", N, N

UDF_SPEC = {
    "name": "tpch_q4",
    "description": "TPC-H Q4 — Order Priority Checking",
    "setup": setup,
}