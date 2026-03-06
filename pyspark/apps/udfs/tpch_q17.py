# udfs/tpch_q17.py
"""TPC-H Q17 — Small-Quantity-Order Revenue
原始 SQL：找特定 brand+container 的零件，数量低于该零件平均数量 0.2 倍的行。
UDF：每行做 brand/container 匹配 + 数量阈值比较。
（注：原始 Q17 需要子查询计算 avg，这里用固定阈值模拟逐行计算。）
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("revenue_part",  DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    brands = sql_array(BRANDS)
    containers = sql_array(CONTAINERS)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({brands}, CAST((id % {len(BRANDS)}) + 1 AS INT)) AS p_brand",
        f"element_at({containers}, CAST((id % {len(CONTAINERS)}) + 1 AS INT)) AS p_container",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        # 模拟该零件的平均数量
        "CAST(20 + (id % 10) AS DOUBLE) AS avg_quantity",
    )


def _make_udf():
    _target_brand = "Brand#23"
    _target_container = "MED BOX"

    def fn(row_id, p_brand, p_container, l_quantity,
           l_extendedprice, avg_quantity, _java_ts):
        t0 = time.perf_counter_ns()
        if (p_brand == _target_brand
                and p_container == _target_container
                and l_quantity < 0.2 * avg_quantity):
            revenue = l_extendedprice
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (revenue, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.revenue_part, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, p_brand, p_container, l_quantity,
                l_extendedprice, avg_quantity, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q17", N, N

UDF_SPEC = {
    "name": "tpch_q17",
    "description": "TPC-H Q17 — Small-Quantity-Order Revenue",
    "setup": setup,
}