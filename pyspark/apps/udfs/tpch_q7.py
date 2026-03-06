# udfs/tpch_q7.py
"""TPC-H Q7 — Volume Shipping
原始 SQL：双国家间贸易量（FRANCE↔GERMANY），按年分组。
UDF：每行做双向国家对匹配 + 日期范围 + revenue 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("supp_nation",  StringType(), True),
    StructField("cust_nation",  StringType(), True),
    StructField("l_year",       LongType(),   True),
    StructField("volume",       DoubleType(), True),
    StructField("passed_filter", LongType(),  True),
    StructField("row_id",       LongType(),   True),
    StructField("py_duration",  LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"element_at({nations}, CAST(((id + 7) % {len(NATIONS)}) + 1 AS INT)) AS c_nation",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    _n1, _n2 = "FRANCE", "GERMANY"
    # 1995-01-01 = 9131, 1996-12-31 = 9861
    _start = 9131
    _end = 9861

    def fn(row_id, l_extendedprice, l_discount,
           s_nation, c_nation, l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if _start <= l_shipdate_days <= _end:
            if (s_nation == _n1 and c_nation == _n2) or \
               (s_nation == _n2 and c_nation == _n1):
                volume = l_extendedprice * (1.0 - l_discount)
                # 计算年份: epoch days → year
                l_year = 1970 + l_shipdate_days // 365
                passed = 1
                elapsed = time.perf_counter_ns() - t0
                return (s_nation, c_nation, l_year, volume,
                        passed, row_id, elapsed)
        elapsed = time.perf_counter_ns() - t0
        return (None, None, None, 0.0, 0, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.supp_nation, r.cust_nation, r.l_year, r.volume, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount,
                s_nation, c_nation, l_shipdate_days,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q7", N, N

UDF_SPEC = {
    "name": "tpch_q7",
    "description": "TPC-H Q7 — Volume Shipping",
    "setup": setup,
}