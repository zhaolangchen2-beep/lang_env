"""TPC-H Q19 — Discounted Revenue（最复杂多条件过滤）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_BRANDS = [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)]
_CONTAINERS = [
    f"{sz} {tp}"
    for sz in ["SM","MED","LG","WRAP","JUMBO"]
    for tp in ["CASE","BOX","PACK","PKG","BAG","JAR","CAN","DRUM"]
]
_SHIP_MODES = ["REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"]
_SHIP_INSTRUCTS = [
    "DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"
]

_schema = StructType([
    StructField("revenue_part", DoubleType(), True),
    StructField("row_id",       LongType(),  True),
    StructField("py_duration",  LongType(),  True),
])


def _gen_lineitem(spark, n_rows, parallelism):
    br = "array(" + ",".join(f"'{s}'" for s in _BRANDS) + ")"
    co = "array(" + ",".join(f"'{s}'" for s in _CONTAINERS) + ")"
    sm = "array(" + ",".join(f"'{s}'" for s in _SHIP_MODES) + ")"
    si = "array(" + ",".join(f"'{s}'" for s in _SHIP_INSTRUCTS) + ")"
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        f"element_at({sm}, CAST((id % {len(_SHIP_MODES)}) + 1 AS INT)) AS l_shipmode",
        f"element_at({si}, CAST((id % {len(_SHIP_INSTRUCTS)}) + 1 AS INT)) AS l_shipinstruct",
        f"element_at({br}, CAST((id % {len(_BRANDS)}) + 1 AS INT)) AS p_brand",
        f"element_at({co}, CAST((id % {len(_CONTAINERS)}) + 1 AS INT)) AS p_container",
        "CAST(1 + (id % 50) AS INT) AS p_size",
    )


def _make_udf():
    _sm = frozenset({"SM CASE","SM BOX","SM PACK","SM PKG"})
    _med = frozenset({"MED BAG","MED BOX","MED PKG","MED PACK"})
    _lg = frozenset({"LG CASE","LG BOX","LG PACK","LG PKG"})
    _air = frozenset({"AIR","AIR REG"})

    def fn(row_id, l_extendedprice, l_discount, l_quantity,
           l_shipmode, l_shipinstruct,
           p_brand, p_container, p_size, _java_ts):
        t0 = time.perf_counter_ns()
        revenue = 0.0
        if l_shipinstruct == "DELIVER IN PERSON" and l_shipmode in _air:
            if (p_brand == "Brand#12" and p_container in _sm
                    and 1 <= l_quantity <= 11 and 1 <= p_size <= 5):
                revenue = l_extendedprice * (1.0 - l_discount)
            elif (p_brand == "Brand#23" and p_container in _med
                  and 10 <= l_quantity <= 20 and 1 <= p_size <= 10):
                revenue = l_extendedprice * (1.0 - l_discount)
            elif (p_brand == "Brand#34" and p_container in _lg
                  and 20 <= l_quantity <= 30 and 1 <= p_size <= 15):
                revenue = l_extendedprice * (1.0 - l_discount)
        elapsed = time.perf_counter_ns() - t0
        return (revenue, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    li = _gen_lineitem(spark, N, args.parallelism)

    spark.udf.register("process", _make_udf())
    li.createOrReplaceTempView("src")

    sql = """
        SELECT r.revenue_part,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount, l_quantity,
                l_shipmode, l_shipinstruct,
                p_brand, p_container, p_size,
                mark_start(l_rowid)
            ) AS r
            FROM src
        ) t
    """
    return sql, "TPC-H Q19", N, N


UDF_SPEC = {
    "name":        "tpch_q19",
    "description": "TPC-H Q19 — Discounted Revenue (complex filter)",
    "setup":       setup,
}