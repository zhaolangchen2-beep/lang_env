# udfs/exp_str16_arrow.py """实验：18 列，16 个字符串（Arrow Pandas UDF 适配版）"""

import time
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, LongType, StringType

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
    # 注意：参数类型变成了 pd.Series
    @pandas_udf(_schema)
    def fn(row_id: pd.Series, 
           s_in1: pd.Series, s_in2: pd.Series, s_in3: pd.Series, s_in4: pd.Series, 
           s_in5: pd.Series, s_in6: pd.Series, s_in7: pd.Series, s_in8: pd.Series, 
           _java_ts: pd.Series) -> pd.DataFrame:
        
        # 记录整个 Batch 的开始时间
        t0 = time.perf_counter_ns()
        
        # 构造返回的 DataFrame（列名和顺序必须与 _schema 严格一致）
        res_df = pd.DataFrame({
            "s_out1": s_in1,
            "s_out2": s_in2,
            "s_out3": s_in3,
            "s_out4": s_in4,
            "s_out5": s_in5,
            "s_out6": s_in6,
            "s_out7": s_in7,
            "s_out8": s_in8,
            "row_id": row_id
        })
        
        # 计算整个批次的耗时，并均摊到每一行（为了与原 benchmark SQL 兼容）
        elapsed_total = time.perf_counter_ns() - t0
        batch_size = len(row_id)
        # 将均摊耗时作为一列返回
        res_df["py_duration"] = int(elapsed_total / batch_size) 
        
        return res_df

    return fn

def setup(spark, args):
    # 开启 Pandas UDF 所需的底层 Arrow 配置
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    N = args.row_count
    # 数据生成逻辑不变
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
    
    # 注册 Pandas UDF
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    
    # SQL 逻辑不变
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
    return sql, "EXP_STR16_ARROW (16 strings Vectorized)", N, N

UDF_SPEC = {
    "name": "exp_str16_arrow",
    "description": "Experiment: 18 fields, 16 strings (8 in, 8 out)",
    "setup": setup,
}