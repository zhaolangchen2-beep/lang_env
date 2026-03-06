# udfs/tpch_q21.py
"""TPC-H Q21 — Suppliers Who Kept Orders Waiting
原始 SQL：找延迟交付且是唯一延迟供应商的沙特阿拉伯供应商。
UDF：每行做国家过滤 + 订单状态检查 + 延迟判断 + 多供应商标记。
（注：原始需要 EXISTS/NOT EXISTS 子查询，这里用预标记模拟。）
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("s_name",        StringType(), True),
    StructField("numwait",       LongType(),   True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    status = sql_array(ORDER_STATUS)
    n_supp = max(n_rows // 600, 100)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"concat('Supplier#', CAST(id % {n_supp} AS STRING)) AS s_name",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"element_at({status}, CAST((id % {len(ORDER_STATUS)}) + 1 AS INT)) AS o_orderstatus",
        "datediff(date_add(DATE '1992-01-01', CAST((id + 60) % 2556 AS INT)), DATE '1970-01-01') AS l_receiptdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id + 30) % 2556 AS INT)), DATE '1970-01-01') AS l_commitdate_days",
        # 模拟：该订单是否有多个供应商（用于 EXISTS 子查询语义）
        "CAST(CASE WHEN id % 3 = 0 THEN 1 ELSE 0 END AS INT) AS has_other_supplier",
        # 模拟：其他供应商是否也延迟了（用于 NOT EXISTS 子查询语义）
        "CAST(CASE WHEN id % 7 = 0 THEN 1 ELSE 0 END AS INT) AS other_also_late",
    )


def _make_udf():
    _target = "SAUDI ARABIA"

    def fn(row_id, s_name, s_nation, o_orderstatus,
           l_receiptdate_days, l_commitdate_days,
           has_other_supplier, other_also_late, _java_ts):
        t0 = time.perf_counter_ns()
        # 条件：沙特供应商 + 订单状态F + 自己延迟 + 有其他供应商 + 其他没延迟
        if (s_nation == _target
                and o_orderstatus == "F"
                and l_receiptdate_days > l_commitdate_days
                and has_other_supplier == 1
                and other_also_late == 0):
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (s_name, passed, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_name, r.numwait, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, s_name, s_nation, o_orderstatus,
                l_receiptdate_days, l_commitdate_days,
                has_other_supplier, other_also_late,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q21", N, N

UDF_SPEC = {
    "name": "tpch_q21",
    "description": "TPC-H Q21 — Suppliers Who Kept Orders Waiting",
    "setup": setup,
}