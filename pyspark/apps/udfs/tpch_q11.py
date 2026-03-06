# udfs/tpch_q11.py
"""TPC-H Q11 — Important Stock Identification
原始 SQL：找德国供应商的零件，按 supplycost*availqty 值排序。
UDF：每行做国家过滤 + value 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("ps_partkey",    LongType(),   True),
    StructField("part_value",    DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(id % 200000 AS BIGINT) AS ps_partkey",
        "CAST(1.0 + (id % 1000) * 0.01 AS DOUBLE) AS ps_supplycost",
        "CAST(1 + (id % 9999) AS BIGINT) AS ps_availqty",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
    )


def _make_udf():
    _target = "GERMANY"

    def fn(row_id, ps_partkey, ps_supplycost, ps_availqty,
           s_nation, _java_ts):
        t0 = time.perf_counter_ns()
        if s_nation == _target:
            part_value = ps_supplycost * ps_availqty
            passed = 1
        else:
            part_value = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (ps_partkey, part_value, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.ps_partkey, r.part_value, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, ps_partkey, ps_supplycost, ps_availqty,
                s_nation, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q11", N, N

UDF_SPEC = {
    "name": "tpch_q11",
    "description": "TPC-H Q11 — Important Stock Identification",
    "setup": setup,
}