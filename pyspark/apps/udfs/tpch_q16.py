# udfs/tpch_q16.py
"""TPC-H Q16 — Parts/Supplier Relationship
原始 SQL：排除投诉供应商，按 brand/type/size 统计不同供应商数量。
UDF：每行做 brand 排除 + 多值 size 匹配 + 投诉标记检查。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("p_brand",       StringType(), True),
    StructField("p_type",        StringType(), True),
    StructField("p_size",        LongType(),   True),
    StructField("ps_suppkey",    LongType(),   True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    brands = sql_array(BRANDS)
    types_sql = sql_array(PART_TYPES)
    n_supp = max(n_rows // 600, 100)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({brands}, CAST((id % {len(BRANDS)}) + 1 AS INT)) AS p_brand",
        f"element_at({types_sql}, CAST((id % {len(PART_TYPES)}) + 1 AS INT)) AS p_type",
        "CAST(1 + (id % 50) AS INT) AS p_size",
        f"CAST(id % {n_supp} AS BIGINT) AS ps_suppkey",
        # 模拟投诉供应商：每 100 个中有 1 个
        "CAST(CASE WHEN id % 100 = 0 THEN 1 ELSE 0 END AS INT) AS is_complaint",
    )


def _make_udf():
    _exclude_brand = "Brand#45"
    _exclude_type_prefix = "MEDIUM POLISHED"
    _target_sizes = frozenset({49, 14, 23, 45, 19, 3, 36, 9})

    def fn(row_id, p_brand, p_type, p_size, ps_suppkey,
           is_complaint, _java_ts):
        t0 = time.perf_counter_ns()
        if (p_brand != _exclude_brand
                and not p_type.startswith(_exclude_type_prefix)
                and p_size in _target_sizes
                and is_complaint == 0):
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (p_brand, p_type, p_size, ps_suppkey,
                passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.p_brand, r.p_type, r.p_size, r.ps_suppkey, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, p_brand, p_type, p_size, ps_suppkey,
                is_complaint, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q16", N, N

UDF_SPEC = {
    "name": "tpch_q16",
    "description": "TPC-H Q16 — Parts/Supplier Relationship",
    "setup": setup,
}