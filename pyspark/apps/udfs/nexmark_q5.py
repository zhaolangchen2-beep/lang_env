"""NexMark Q5 — Hot Items with HOP sliding window"""

import time
from collections import defaultdict
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType

_WIN_SIZE_MS = 10_000
_WIN_SLIDE_MS = 2_000

_schema = StructType([
    StructField("auction",     LongType(), True),
    StructField("num",         LongType(), True),
    StructField("bid_id",      LongType(), True),
    StructField("py_duration", LongType(), True),
])


def _gen_bids(spark, n_rows, parallelism):
    n_auctions = max(n_rows // 100, 1000)
    n_hot = max(n_auctions // 100, 1)
    n_cold = max(n_auctions - n_hot, 1)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS bid_id",
        f"""CASE WHEN id % 2 = 0
                 THEN CAST(id % {n_hot}  AS BIGINT)
                 ELSE CAST({n_hot} + id % {n_cold} AS BIGINT)
           END AS auction""",
        f"CAST(id * {max(60000 // n_rows, 1)} AS BIGINT) AS date_time",
    )


def _make_udf(all_bids):
    _raw = all_bids
    _ws = _WIN_SIZE_MS
    _wsl = _WIN_SLIDE_MS

    def fn(bid_id, auction, date_time, _java_ts):
        t0 = time.perf_counter_ns()
        if not hasattr(fn, "_hot"):
            ts_list = [b[2] for b in _raw]
            epoch = (min(ts_list) // _wsl) * _wsl
            awc, awb = defaultdict(int), defaultdict(list)
            for b_id, b_auc, b_ts in _raw:
                k_min = max(0, ((b_ts - _ws - epoch) // _wsl) + 1)
                k_max = (b_ts - epoch) // _wsl
                for k in range(k_min, k_max + 1):
                    ws = epoch + k * _wsl
                    we = ws + _ws
                    if ws <= b_ts < we:
                        key = (b_auc, ws, we)
                        awc[key] += 1
                        awb[key].append(b_id)
            wmax = defaultdict(int)
            for (auc, ws, we), cnt in awc.items():
                wk = (ws, we)
                if cnt > wmax[wk]:
                    wmax[wk] = cnt
            hot = {}
            for (auc, ws, we), cnt in awc.items():
                if cnt >= wmax[(ws, we)]:
                    for b_id in awb[(auc, ws, we)]:
                        if b_id not in hot or hot[b_id][1] < cnt:
                            hot[b_id] = (auc, cnt)
            fn._hot = hot

        r = fn._hot.get(bid_id)
        if r is None:
            return (None, None, None, time.perf_counter_ns() - t0)
        return (r[0], r[1], bid_id, time.perf_counter_ns() - t0)

    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    bids_df = _gen_bids(spark, N, args.parallelism)

    print("Collecting all bids …")
    all_bids = [(r.bid_id, r.auction, r.date_time) for r in bids_df.collect()]
    print(f"  bids : {len(all_bids):,}")

    spark.udf.register("process", _make_udf(all_bids))
    bids_df.createOrReplaceTempView("src")

    sql = """
        SELECT r.auction, r.num,
               calc_overhead(r.bid_id, r.py_duration) AS overhead
        FROM (
            SELECT process(bid_id, auction, date_time, mark_start(bid_id)) AS r
            FROM src
        ) t
    """
    return sql, "NexMark Q5", N, -1


UDF_SPEC = {
    "name":        "q5",
    "description": "NexMark Q5 — Hot Items (HOP sliding window)",
    "setup":       setup,
}