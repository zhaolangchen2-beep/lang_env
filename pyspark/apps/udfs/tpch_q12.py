"""TPC-H Q12 — Shipping Modes and Order Priority"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

_SHIP_MODES = ["REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"]
_ORDER_PRIOS = ["1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"]

_schema = StructType([
    StructField("l_shipmode",    StringType(), True),
    StructField("high_line",     LongType(),   True),
    StructField("low_line",      LongType(),   True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_lineitem(spark, n_rows, parallelism):
    sm = "array(" + ",".join(f"'{s}'" for s in _SHIP_MODES) + ")"
    op = "array(" + ",".join(f"'{s}'" for s in _ORDER_PRIOS) + ")"
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({sm}, CAST((id % {len(_SHIP_MODES)}) + 1 AS INT)) AS l_shipmode",
        f"element_at({op}, CAST((id % {len(_ORDER_PRIOS)}) + 1 AS INT)) AS o_orderpriority",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_shipdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id+30) % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_commitdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id+60) % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_receiptdate_days",
    )


def _make_udf():
    _target = frozenset({"MAIL", "SHIP"})
    _high = frozenset({"1-URGENT", "2-HIGH"})

    def fn(row_id, l_shipmode, o_orderpriority,
           l_commitdate_days, l_receiptdate_days, l_shipdate_days,
           _java_ts):
        t0 = time.perf_counter_ns()
        if (l_shipmode in _target
                and l_commitdate_days < l_receiptdate_days
                and l_shipdate_days < l_commitdate_days
                and 8766 <= l_receiptdate_days < 9131):
            if o_orderpriority in _high:
                h, lo = 1, 0
            else:
                h, lo = 0, 1
            p = 1
        else:
            h, lo, p = 0, 0, 0
        elapsed = time.perf_counter_ns() - t0
        return (l_shipmode, h, lo, p, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    li = _gen_lineitem(spark, N, args.parallelism)

    spark.udf.register("process", _make_udf())
    li.createOrReplaceTempView("src")

    sql = """
        SELECT r.l_shipmode, r.high_line, r.low_line, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_shipmode, o_orderpriority,
                l_commitdate_days, l_receiptdate_days, l_shipdate_days,
                mark_start(l_rowid)
            ) AS r
            FROM src
        ) t
    """
    return sql, "TPC-H Q12", N, N


UDF_SPEC = {
    "name":        "tpch_q12",
    "description": "TPC-H Q12 — Shipping Modes and Order Priority",
    "setup":       setup,
}