# udfs/tpch_q10.py
"""TPC-H Q10 — Returned Item Reporting
原始 SQL：找退货（returnflag='R'）的客户及其退货金额，按金额降序。
UDF：每行做 returnflag + 日期过滤 + revenue 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("c_custkey",     LongType(),   True),
    StructField("c_name",        StringType(), True),
    StructField("c_nation",      StringType(), True),
    StructField("revenue",       DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    n_custs = max(n_rows // 10, 1000)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"CAST(id % {n_custs} AS BIGINT) AS c_custkey",
        f"concat('Customer#', CAST(id % {n_custs} AS STRING)) AS c_name",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS c_nation",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "element_at(array('A','R','N'), CAST((id % 3) + 1 AS INT)) AS l_returnflag",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
    )


def _make_udf():
    # 1993-10-01 = 8674, 1994-01-01 = 8766
    _start = 8674
    _end = 8766

    def fn(row_id, c_custkey, c_name, c_nation,
           l_extendedprice, l_discount, l_returnflag,
           o_orderdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if (l_returnflag == "R"
                and _start <= o_orderdate_days < _end):
            revenue = l_extendedprice * (1.0 - l_discount)
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (c_custkey, c_name, c_nation, revenue,
                passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.c_custkey, r.c_name, r.c_nation, r.revenue, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, c_custkey, c_name, c_nation,
                l_extendedprice, l_discount, l_returnflag,
                o_orderdate_days, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q10", N, N

UDF_SPEC = {
    "name": "tpch_q10",
    "description": "TPC-H Q10 — Returned Item Reporting",
    "setup": setup,
}