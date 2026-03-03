"""TPC-H Q14 — Promotion Effect（字符串前缀匹配 + 条件算术）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_PART_TYPES = [
    "ECONOMY ANODIZED STEEL","ECONOMY BURNISHED COPPER",
    "PROMO ANODIZED TIN","PROMO BURNISHED STEEL",
    "PROMO PLATED NICKEL","STANDARD POLISHED BRASS",
    "SMALL BRUSHED NICKEL","LARGE BURNISHED COPPER",
    "ECONOMY PLATED STEEL","PROMO BRUSHED TIN",
    "STANDARD ANODIZED BRASS","MEDIUM POLISHED COPPER",
]

_schema = StructType([
    StructField("promo_value", DoubleType(), True),
    StructField("total_value", DoubleType(), True),
    StructField("row_id",      LongType(),  True),
    StructField("py_duration", LongType(),  True),
])


def _gen_lineitem(spark, n_rows, parallelism):
    pt = "array(" + ",".join(f"'{s}'" for s in _PART_TYPES) + ")"
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        f"element_at({pt}, CAST((id % {len(_PART_TYPES)}) + 1 AS INT)) AS p_type",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    def fn(row_id, l_extendedprice, l_discount,
           p_type, l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        # 1995-09-01=9374, 1995-10-01=9404
        if 9374 <= l_shipdate_days < 9404:
            val = l_extendedprice * (1.0 - l_discount)
            promo = val if p_type.startswith("PROMO") else 0.0
        else:
            val, promo = 0.0, 0.0
        elapsed = time.perf_counter_ns() - t0
        return (promo, val, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    li = _gen_lineitem(spark, N, args.parallelism)

    spark.udf.register("process", _make_udf())
    li.createOrReplaceTempView("src")

    sql = """
        SELECT r.promo_value, r.total_value,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount, p_type,
                l_shipdate_days, mark_start(l_rowid)
            ) AS r
            FROM src
        ) t
    """
    return sql, "TPC-H Q14", N, N


UDF_SPEC = {
    "name":        "tpch_q14",
    "description": "TPC-H Q14 — Promotion Effect (string matching)",
    "setup":       setup,
}