# udfs/tpch_q8.py
"""TPC-H Q8 — National Market Share
原始 SQL：八表 JOIN，计算特定国家在区域市场中的份额，按年。
UDF：每行做 region/type 过滤 + 国家标记 + volume 计算。
"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import *

_schema = StructType([
    StructField("o_year",        LongType(),   True),
    StructField("volume",        DoubleType(), True),
    StructField("nation_volume", DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    nations = sql_array(NATIONS)
    regions = sql_array(REGIONS)
    types_sql = sql_array(PART_TYPES)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        f"element_at({nations}, CAST((id % {len(NATIONS)}) + 1 AS INT)) AS s_nation",
        f"element_at({regions}, CAST((id % {len(REGIONS)}) + 1 AS INT)) AS c_region",
        f"element_at({types_sql}, CAST((id % {len(PART_TYPES)}) + 1 AS INT)) AS p_type",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
    )


def _make_udf():
    _target_nation = "BRAZIL"
    _target_region = "AMERICA"
    _target_type = "ECONOMY ANODIZED STEEL"
    # 1995-01-01 = 9131, 1996-12-31 = 9861
    _start = 9131
    _end = 9861

    def fn(row_id, l_extendedprice, l_discount,
           s_nation, c_region, p_type, o_orderdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if (c_region == _target_region
                and p_type == _target_type
                and _start <= o_orderdate_days <= _end):
            volume = l_extendedprice * (1.0 - l_discount)
            o_year = 1970 + o_orderdate_days // 365
            nation_vol = volume if s_nation == _target_nation else 0.0
            passed = 1
        else:
            o_year = 0
            volume = 0.0
            nation_vol = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (o_year, volume, nation_vol, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.o_year, r.volume, r.nation_volume, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount,
                s_nation, c_region, p_type, o_orderdate_days,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q8", N, N

UDF_SPEC = {
    "name": "tpch_q8",
    "description": "TPC-H Q8 — National Market Share",
    "setup": setup,
}