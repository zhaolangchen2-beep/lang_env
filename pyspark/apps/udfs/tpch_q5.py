# udfs/tpch_q5.py
"""TPC-H Q5 — Local Supplier Volume
原始 SQL：六表 JOIN，按 region 过滤，计算 revenue。
UDF：每行做 region 匹配 + nation 一致性校验 + revenue 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("n_name",        StringType(), True),
    StructField("revenue",       DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    regions = sql_array(REGIONS)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS c_nation",
        f"element_at({nations}, CAST(((id + 3) % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"element_at({regions}, CAST((id % {len(REGIONS)}) + 1 AS INT)) AS r_name",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
    )


def _make_udf():
    _target_region = "ASIA"
    _nation_region = NATION_TO_REGION
    # 1994-01-01 = 8766, 1995-01-01 = 9131
    _start = 8766
    _end = 9131

    def fn(row_id, l_extendedprice, l_discount,
           c_nation, s_nation, r_name, o_orderdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if (r_name == _target_region
                and c_nation == s_nation
                and _start <= o_orderdate_days < _end):
            revenue = l_extendedprice * (1.0 - l_discount)
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (c_nation, revenue, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.n_name, r.revenue, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount,
                c_nation, s_nation, r_name, o_orderdate_days,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q5", N, N

UDF_SPEC = {
    "name": "tpch_q5",
    "description": "TPC-H Q5 — Local Supplier Volume",
    "setup": setup,
}