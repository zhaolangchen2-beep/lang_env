# udfs/tpch_q22.py
"""TPC-H Q22 — Global Sales Opportunity
原始 SQL：找特定国家代码的客户中，余额高于平均且 7 年无订单的客户。
UDF：每行做国家代码前缀匹配 + 余额阈值比较 + 无订单标记检查。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("cntrycode",     StringType(), True),
    StructField("c_acctbal",     DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])

_COUNTRY_CODES = ["13", "31", "23", "29", "30", "18", "17"]


def _gen_data(spark, n_rows, parallelism):
    # 电话号码前两位作为国家代码
    codes = sql_array(_COUNTRY_CODES + ["10", "11", "12", "14", "15", "16", "19", "20"])
    n_custs = max(n_rows // 5, 1000)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({codes}, CAST((id % 14) + 1 AS INT)) AS cntrycode",
        "CAST(-500.0 + (id % 15000) * 0.1 AS DOUBLE) AS c_acctbal",
        # 模拟：客户是否有订单（70% 有订单，30% 无订单）
        "CAST(CASE WHEN id % 10 < 3 THEN 0 ELSE 1 END AS INT) AS has_orders",
        # 模拟全局平均余额
        "CAST(500.0 AS DOUBLE) AS avg_acctbal",
    )


def _make_udf():
    _target_codes = frozenset({"13", "31", "23", "29", "30", "18", "17"})

    def fn(row_id, cntrycode, c_acctbal, has_orders,
           avg_acctbal, _java_ts):
        t0 = time.perf_counter_ns()
        if (cntrycode in _target_codes
                and c_acctbal > avg_acctbal
                and has_orders == 0):
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (cntrycode, c_acctbal, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.cntrycode, r.c_acctbal, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, cntrycode, c_acctbal, has_orders,
                avg_acctbal, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q22", N, N

UDF_SPEC = {
    "name": "tpch_q22",
    "description": "TPC-H Q22 — Global Sales Opportunity",
    "setup": setup,
}