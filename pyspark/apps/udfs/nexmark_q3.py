"""NexMark Q3 — JOIN auctions × persons + filter"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

# ── 本 case 私有常量 ─────────────────────────────────────────
_US_STATES = [
    "OR","ID","CA","WA","NY","TX","FL","IL","PA","OH",
    "GA","NC","MI","NJ","VA","AZ","MA","TN","IN","MO",
    "MD","WI","CO","MN","SC","AL","LA","KY","OK","CT",
    "UT","IA","NV","AR","MS","KS","NM","NE","WV","HI",
    "NH","ME","MT","RI","DE","SD","ND","AK","VT","WY",
]
_NUM_CATEGORIES = 20
_TARGET_CATEGORY = 10
_TARGET_STATES = frozenset({"OR", "ID", "CA"})

_schema = StructType([
    StructField("name",        StringType(), True),
    StructField("city",        StringType(), True),
    StructField("state",       StringType(), True),
    StructField("auction_id",  LongType(),   True),
    StructField("py_duration", LongType(),   True),
])


# ── 数据生成（自包含） ───────────────────────────────────────
def _gen_persons(spark, n_pers, parallelism):
    sa = "array(" + ",".join(f"'{s}'" for s in _US_STATES) + ")"
    return spark.range(0, n_pers, numPartitions=parallelism).selectExpr(
        "id AS p_id",
        "concat('person_', id) AS name",
        "concat('city_', CAST(id % 1000 AS STRING)) AS city",
        f"element_at({sa}, CAST((id % {len(_US_STATES)}) + 1 AS INT)) AS state",
    )


def _gen_auctions(spark, n_rows, n_pers, parallelism):
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS a_id",
        f"CAST(id % {n_pers} AS BIGINT) AS seller",
        f"CAST(id % {_NUM_CATEGORIES} AS INT) AS category",
    )


# ── UDF ───────────────────────────────────────────────────────
def _make_udf(all_persons):
    _raw = all_persons

    def fn(a_id, seller, category, _java_ts):
        t0 = time.perf_counter_ns()
        if not hasattr(fn, "_pdict"):
            fn._pdict = {p_id: (name, city, state)
                         for p_id, name, city, state in _raw}

        r = fn._pdict.get(seller)
        if r is None:
            return (None, None, None, None, time.perf_counter_ns() - t0)
        name, city, state = r
        if category != _TARGET_CATEGORY or state not in _TARGET_STATES:
            return (None, None, None, None, time.perf_counter_ns() - t0)
        return (name, city, state, a_id, time.perf_counter_ns() - t0)

    return udf(fn, _schema)


# ── setup ─────────────────────────────────────────────────────
def setup(spark, args):
    N = args.row_count
    n_pers = max(N // args.person_ratio, 1000)

    persons_df = _gen_persons(spark, n_pers, args.parallelism)
    auctions_df = _gen_auctions(spark, N, n_pers, args.parallelism)

    print("Collecting persons …")
    all_persons = [(r.p_id, r.name, r.city, r.state) for r in persons_df.collect()]
    print(f"  persons : {len(all_persons):,}")

    spark.udf.register("process", _make_udf(all_persons))
    auctions_df.createOrReplaceTempView("src")

    sql = """
        SELECT r.name, r.city, r.state, r.auction_id,
               calc_overhead(r.auction_id, r.py_duration) AS overhead
        FROM (
            SELECT process(a_id, seller, category, mark_start(a_id)) AS r
            FROM src
        ) t
    """
    sel = len(_TARGET_STATES) / len(_US_STATES) / _NUM_CATEGORIES
    return sql, "NexMark Q3", N, int(N * sel)


UDF_SPEC = {
    "name":        "q3",
    "description": "NexMark Q3 — JOIN auctions×persons + filter",
    "setup":       setup,
}