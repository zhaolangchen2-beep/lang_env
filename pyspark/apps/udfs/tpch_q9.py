# udfs/tpch_q9.py
"""TPC-H Q9 — Product Type Profit Measure
原始 SQL：六表 JOIN，按 part name LIKE '%green%' 过滤，
         计算 profit = extprice*(1-discount) - supplycost*quantity，按国家和年分组。
UDF：每行做字符串包含匹配 + profit 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("nation",        StringType(), True),
    StructField("o_year",        LongType(),   True),
    StructField("profit",        DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    colors = sql_array(PART_COLORS)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        "CAST(1.0 + (id % 1000) * 0.01 AS DOUBLE) AS ps_supplycost",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"concat('part_', element_at({colors}, CAST((id % {len(PART_COLORS)}) + 1 AS INT))) AS p_name",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
    )


def _make_udf():
    _target_color = "green"

    def fn(row_id, l_extendedprice, l_discount, l_quantity,
           ps_supplycost, s_nation, p_name, o_orderdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if _target_color in p_name:
            profit = l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity
            o_year = 1970 + o_orderdate_days // 365
            passed = 1
        else:
            profit = 0.0
            o_year = 0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (s_nation, o_year, profit, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.nation, r.o_year, r.profit, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount, l_quantity,
                ps_supplycost, s_nation, p_name, o_orderdate_days,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q9", N, N

UDF_SPEC = {
    "name": "tpch_q9",
    "description": "TPC-H Q9 — Product Type Profit Measure",
    "setup": setup,
}