# udfs/tpch_q13.py
"""TPC-H Q13 — Customer Distribution
原始 SQL：统计每个客户的订单数（排除 comment LIKE '%special%requests%'），
         再按订单数分组统计客户数。
UDF：每行做 comment 模式匹配 + 计数标记。
"""

import time
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

_schema = StructType([
    StructField("c_custkey",     LongType(),   True),
    StructField("is_valid",      LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])

_COMMENTS = [
    "regular accounts are carefully",
    "special requests about the slyly pending",
    "pending deposits boost carefully special requests",
    "furiously special ideas haggle blithely",
    "ironic foxes detect slyly regular courts",
    "blithely final accounts use evenly",
    "quickly express asymptotes are about the special requests",
    "final deposits cajole carefully special packages",
]


def _gen_data(spark, n_rows, parallelism):
    comments = "array(" + ",".join(f"'{c}'" for c in _COMMENTS) + ")"
    n_custs = max(n_rows // 10, 1000)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"CAST(id % {n_custs} AS BIGINT) AS c_custkey",
        f"element_at({comments}, CAST((id % {len(_COMMENTS)}) + 1 AS INT)) AS o_comment",
    )


def _make_udf():
    _pattern = re.compile(r"special.*requests")

    def fn(row_id, c_custkey, o_comment, _java_ts):
        t0 = time.perf_counter_ns()
        if _pattern.search(o_comment):
            is_valid = 0  # 排除
        else:
            is_valid = 1
        elapsed = time.perf_counter_ns() - t0
        return (c_custkey, is_valid, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.c_custkey, r.is_valid,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, c_custkey, o_comment, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q13", N, N

UDF_SPEC = {
    "name": "tpch_q13",
    "description": "TPC-H Q13 — Customer Distribution",
    "setup": setup,
}