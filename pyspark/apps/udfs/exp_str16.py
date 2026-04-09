# udfs/exp_str16.py """实验：18 列，16 个字符串（输入8+输出8）"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

# 定义输出结构：8个字符串输出 + row_id + 耗时
_schema = StructType([
    StructField("s_out1", StringType(), True),
    StructField("s_out2", StringType(), True),
    StructField("s_out3", StringType(), True),
    StructField("s_out4", StringType(), True),
    StructField("s_out5", StringType(), True),
    StructField("s_out6", StringType(), True),
    StructField("s_out7", StringType(), True),
    StructField("s_out8", StringType(), True),
    StructField("row_id", LongType(), True),
    StructField("py_duration", LongType(), True),
])

def _make_udf():
    # 输入参数：row_id, 8个字符串输入, _java_ts
    def fn(row_id, s_in1, s_in2, s_in3, s_in4, s_in5, s_in6, s_in7, s_in8, _java_ts):
        t0 = time.perf_counter_ns()
        
        # 模拟处理：这里直接透传输入作为输出
        res = (s_in1, s_in2, s_in3, s_in4, s_in5, s_in6, s_in7, s_in8, row_id)
        
        elapsed = time.perf_counter_ns() - t0
        return res + (elapsed,)
    
    return udf(fn, _schema)

def setup(spark, args):
    N = args.row_count
    # 生成包含 8 个字符串列的测试数据
    df = spark.range(0, N, numPartitions=args.parallelism).selectExpr(
        "id AS row_id",
        "concat('s1_', CAST(id % 1000 AS STRING)) AS s_in1",
        "concat('s2_', CAST(id % 25 AS STRING)) AS s_in2",
        "concat('s3_', CAST(id % 5 AS STRING)) AS s_in3",
        "concat('s4_', CAST(id % 100 AS STRING)) AS s_in4",
        "concat('s5_', CAST(id % 500 AS STRING)) AS s_in5",
        "concat('s6_', CAST(id % 200 AS STRING)) AS s_in6",
        "concat('s7_', CAST(id % 10 AS STRING)) AS s_in7",
        "concat('s8_', CAST(id % 150 AS STRING)) AS s_in8",
    )
    
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    
    # SQL 逻辑：调用 UDF 并展开结果
    sql = """
        SELECT r.s_out1, r.s_out2, r.s_out3, r.s_out4, 
               r.s_out5, r.s_out6, r.s_out7, r.s_out8,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                row_id, 
                s_in1, s_in2, s_in3, s_in4, 
                s_in5, s_in6, s_in7, s_in8, 
                mark_start(row_id)
            ) AS r
            FROM src
        ) t
    """
    return sql, "EXP_STR16 (16 strings)", N, N

UDF_SPEC = {
    "name": "exp_str16",
    "description": "Experiment: 18 fields, 16 strings (8 in, 8 out)",
    "setup": setup,
}