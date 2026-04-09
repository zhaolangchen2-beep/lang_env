# udfs/exp_str6.py
"""实验：10 列，6 个字符串（输入3+输出3）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

_schema = StructType([
    StructField("s_out1", StringType(), True),
    StructField("s_out2", StringType(), True),
    StructField("s_out3", StringType(), True),
    StructField("row_id", LongType(), True),   
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    def fn(row_id, s_in1, s_in2, s_in3, _java_ts):
        t0 = time.perf_counter_ns()
        elapsed = time.perf_counter_ns() - t0
        return (s_in1, s_in2, s_in3, row_id, elapsed)
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "concat('supplier_', CAST(id % 1000 AS STRING)) AS s_in1",
        "concat('nation_', CAST(id % 25 AS STRING)) AS s_in2",
        "concat('region_', CAST(id % 5 AS STRING)) AS s_in3",
    )
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_out1, r.s_out2, r.s_out3,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, s_in1, s_in2, s_in3, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STR6 (6 strings)", N, N

UDF_SPEC = {
    "name": "exp_str6",
    "description": "Experiment: 10 fields, 6 strings",
    "setup": setup,
}