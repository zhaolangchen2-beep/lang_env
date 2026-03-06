# udfs/tpch_q20.py
"""TPC-H Q20 — Potential Part Promotion
原始 SQL：找特定国家的供应商，其供应的零件（name LIKE 'forest%'）
         在指定年份的发货数量超过供应量的 50%。
UDF：每行做国家过滤 + 零件名前缀匹配 + 日期范围 + 数量比较。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("s_name",        StringType(), True),
    StructField("s_address",     StringType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    colors = sql_array(PART_COLORS)
    n_supp = max(n_rows // 600, 100)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"concat('Supplier#', CAST(id % {n_supp} AS STRING)) AS s_name",
        f"concat('addr_', CAST(id % {n_supp} AS STRING)) AS s_address",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"concat(element_at({colors}, CAST((id % {len(PART_COLORS)}) + 1 AS INT)), ' part') AS p_name",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        "CAST(100 + (id % 9900) AS DOUBLE) AS ps_availqty",
        # 模拟该供应商+零件在目标年份的发货总量
        "CAST(CASE WHEN id % 5 = 0 THEN 80 + (id % 50) ELSE 10 + (id % 30) END AS DOUBLE) AS shipped_qty",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    _target_nation = "CANADA"
    _target_prefix = "forest"
    # 1994-01-01 = 8766, 1995-01-01 = 9131
    _start = 8766
    _end = 9131

    def fn(row_id, s_name, s_address, s_nation, p_name,
           l_quantity, ps_availqty, shipped_qty, l_shipdate_days,
           _java_ts):
        t0 = time.perf_counter_ns()
        if (s_nation == _target_nation
                and p_name.startswith(_target_prefix)
                and _start <= l_shipdate_days < _end
                and shipped_qty > 0.5 * ps_availqty):
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (s_name, s_address, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_name, r.s_address, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, s_name, s_address, s_nation, p_name,
                l_quantity, ps_availqty, shipped_qty, l_shipdate_days,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q20", N, N

UDF_SPEC = {
    "name": "tpch_q20",
    "description": "TPC-H Q20 — Potential Part Promotion",
    "setup": setup,
}