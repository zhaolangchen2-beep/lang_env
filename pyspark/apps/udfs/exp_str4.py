# udfs/exp_str4.py
"""实验：10 列，4 个字符串（输入2+输出2）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

_schema = StructType([
    StructField("s_out1", StringType(), True),
    StructField("s_out2", StringType(), True),
    StructField("row_id", LongType(), True),
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    def fn(row_id, a, s_in1, s_in2, _java_ts):
        t0 = time.perf_counter_ns()
        s_out1 = s_in1
        s_out2 = s_in2
        elapsed = time.perf_counter_ns() - t0
        return (s_out1, s_out2, row_id, elapsed)
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "CAST(id * 1.1 AS DOUBLE) AS a",
        "concat('name_', CAST(id % 1000 AS STRING)) AS s_in1",
        "concat('city_', CAST(id % 500 AS STRING)) AS s_in2",
    )
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.s_out1, r.s_out2,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(row_id, a, s_in1, s_in2, mark_start(row_id)) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STR4 (4 strings)", N, N

UDF_SPEC = {
    "name": "exp_str4",
    "description": "Experiment: 10 fields, 4 strings",
    "setup": setup,
}