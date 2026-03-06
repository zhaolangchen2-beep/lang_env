# udfs/tpch_q2.py
"""TPC-H Q2 — Minimum Cost Supplier
原始 SQL：找到指定 region 中、特定 size+type 零件的最低供应成本供应商。
UDF 逻辑：每行做 region/size/type 过滤 + 供应成本比较。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("s_name",      StringType(), True),
    StructField("s_nation",    StringType(), True),
    StructField("p_partkey",   LongType(),   True),
    StructField("p_mfgr",     StringType(), True),
    StructField("ps_supplycost", DoubleType(), True),
    StructField("passed_filter", LongType(),  True),
    StructField("row_id",      LongType(),   True),
    StructField("py_duration", LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations_sql = sql_array(NATIONS)
    regions_sql = sql_array(REGIONS)
    types_sql = sql_array(PART_TYPES)
    n_suppliers = max(n_rows // 100, 100)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"CAST(id % 200000 AS BIGINT) AS p_partkey",
        f"CAST(1 + (id % 50) AS INT) AS p_size",
        f"element_at({types_sql}, CAST((id % {len(PART_TYPES)}) + 1 AS INT)) AS p_type",
        "concat('Manufacturer#', CAST(1 + (id % 5) AS STRING)) AS p_mfgr",
        f"concat('Supplier#', CAST(id % {n_suppliers} AS STRING)) AS s_name",
        f"element_at({nations_sql}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"element_at({regions_sql}, CAST(((id % {len(NATIONS)}) / 5) + 1 AS INT)) AS s_region",
        "CAST(1.0 + (id % 1000) * 0.01 AS DOUBLE) AS ps_supplycost",
    )


def _make_udf():
    _target_region = "EUROPE"
    _target_size = 15
    _target_type_suffix = "BRASS"

    def fn(row_id, p_partkey, p_size, p_type, p_mfgr,
           s_name, s_nation, s_region, ps_supplycost, _java_ts):
        t0 = time.perf_counter_ns()
        if (s_region == _target_region
                and p_size == _target_size
                and p_type.endswith(_target_type_suffix)):
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (s_name, s_nation, p_partkey, p_mfgr,
                ps_supplycost, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_name, r.s_nation, r.p_partkey, r.ps_supplycost, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, p_partkey, p_size, p_type, p_mfgr,
                s_name, s_nation, s_region, ps_supplycost, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q2", N, N

UDF_SPEC = {
    "name": "tpch_q2",
    "description": "TPC-H Q2 — Minimum Cost Supplier",
    "setup": setup,
}