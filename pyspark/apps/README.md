pyspark-udf-benchmark/
├── run.py                       # 入口
├── framework/
│   ├── __init__.py
│   ├── runner.py                # 执行引擎
│   └── registry.py              # UDF 自动发现
├── udfs/
│   ├── __init__.py
│   ├── add.py
│   ├── cdf.py
│   ├── nexmark_q3.py
│   ├── nexmark_q5.py
│   ├── nexmark_q8.py
│   ├── tpch_q1.py
│   ├── tpch_q6.py
│   ├── tpch_q12.py
│   ├── tpch_q14.py
│   └── tpch_q19.py
└── README.md