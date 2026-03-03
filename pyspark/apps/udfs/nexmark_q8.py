"""NexMark Q8 — Monitor New Users (TUMBLE window JOIN)"""

import time
from collections import defaultdict
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

_TUMBLE_MS = 10_000

_schema = StructType([
    StructField("person_id",   LongType(),   True),
    StructField("name",        StringType(), True),
    StructField("starttime",   LongType(),   True),
    StructField("auction_id",  LongType(),   True),
    StructField("py_duration", LongType(),   True),
])


def _gen_persons(spark, n_pers, parallelism):
    return spark.range(0, n_pers, numPartitions=parallelism).selectExpr(
        "id AS p_id",
        "concat('person_', id) AS name",
        f"CAST(id * {max(60000 // n_pers, 1)} AS BIGINT) AS p_date_time",
    )


def _gen_auctions(spark, n_rows, n_pers, parallelism):
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS a_id",
        f"CAST(id % {n_pers} AS BIGINT) AS seller",
        f"CAST(id * {max(60000 // n_rows, 1)} AS BIGINT) AS a_date_time",
    )


def _make_udf(all_persons, all_auctions):
    _rp, _ra, _tm = all_persons, all_auctions, _TUMBLE_MS

    def fn(a_id, seller, a_date_time, _java_ts):
        t0 = time.perf_counter_ns()
        if not hasattr(fn, "_lookup"):
            pw = {}
            for p_id, p_name, p_dt in _rp:
                ws = (p_dt // _tm) * _tm
                pw[(p_id, ws, ws + _tm)] = p_name
            aws = set()
            awb = defaultdict(list)
            for aid, asel, adt in _ra:
                ws = (adt // _tm) * _tm
                k = (asel, ws, ws + _tm)
                aws.add(k)
                awb[k].append(aid)
            lookup = {}
            for (p_id, ws, we), pn in pw.items():
                if (p_id, ws, we) in aws:
                    for aid in awb[(p_id, ws, we)]:
                        if aid not in lookup:
                            lookup[aid] = (p_id, pn, ws)
            fn._lookup = lookup

        r = fn._lookup.get(a_id)
        if r is None:
            return (None, None, None, None, time.perf_counter_ns() - t0)
        return (r[0], r[1], r[2], a_id, time.perf_counter_ns() - t0)

    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    n_pers = max(N // args.person_ratio, 1000)

    persons_df = _gen_persons(spark, n_pers, args.parallelism)
    auctions_df = _gen_auctions(spark, N, n_pers, args.parallelism)

    print("Collecting all persons …")
    all_p = [(r.p_id, r.name, r.p_date_time) for r in persons_df.collect()]
    print(f"  persons  : {len(all_p):,}")

    print("Collecting all auctions …")
    all_a = [(r.a_id, r.seller, r.a_date_time) for r in auctions_df.collect()]
    print(f"  auctions : {len(all_a):,}")

    spark.udf.register("process", _make_udf(all_p, all_a))
    auctions_df.createOrReplaceTempView("src")

    sql = """
        SELECT r.person_id, r.name, r.starttime, r.auction_id,
               calc_overhead(r.auction_id, r.py_duration) AS overhead
        FROM (
            SELECT process(a_id, seller, a_date_time, mark_start(a_id)) AS r
            FROM src
        ) t
    """
    return sql, "NexMark Q8", N, -1


UDF_SPEC = {
    "name":        "q8",
    "description": "NexMark Q8 — Monitor New Users (TUMBLE window JOIN)",
    "setup":       setup,
}