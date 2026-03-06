# udfs/tpch_q18.py
"""TPC-H Q18 — Large Volume Customer
原始 SQL：找总数量超过 300 的订单对应的客户。
UDF：每行做数量阈值比较 + 客户信息输出。
（注：原始 Q18 需要子查询 SUM(quantity)>300，这里用预计算的总量模拟。）
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

_schema = StructType([
    StructField("c_name",         StringType(), True),
    StructField("c_custkey",      LongType(),   True),
    StructField("o_orderkey",     LongType(),   True),
    StructField("o_totalprice",   DoubleType(), True),
    StructField("total_quantity", DoubleType(), True),
    StructField("passed_filter",  LongType(),   True),
    StructField("row_id",         LongType(),   True),
    StructField("py_duration",    LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    n_custs = max(n_rows // 10, 1000)
    n_orders = max(n_rows // 4, 1000)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"concat('Customer#', CAST(id % {n_custs} AS STRING)) AS c_name",
        f"CAST(id % {n_custs} AS BIGINT) AS c_custkey",
        f"CAST(id % {n_orders} AS BIGINT) AS o_orderkey",
        "CAST(10000.00 + (id % 990000) * 0.01 AS DOUBLE) AS o_totalprice",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        # 模拟该订单的总数量：大部分 < 300，少数 >= 300
        "CAST(CASE WHEN id % 20 = 0 THEN 310 + (id % 100) ELSE 50 + (id % 200) END AS DOUBLE) AS order_total_qty",
    )


def _make_udf():
    _threshold = 300.0

    def fn(row_id, c_name, c_custkey, o_orderkey, o_totalprice,
           l_quantity, order_total_qty, _java_ts):
        t0 = time.perf_counter_ns()
        if order_total_qty > _threshold:
            passed = 1
        else:
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (c_name, c_custkey, o_orderkey, o_totalprice,
                order_total_qty, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.c_name, r.c_custkey, r.o_orderkey, r.o_totalprice,
               r.total_quantity, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, c_name, c_custkey, o_orderkey, o_totalprice,
                l_quantity, order_total_qty, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q18", N, N

UDF_SPEC = {
    "name": "tpch_q18",
    "description": "TPC-H Q18 — Large Volume Customer",
    "setup": setup,
}