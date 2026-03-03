"""
UDF 自动发现。

每个 udfs/*.py 必须导出：

    UDF_SPEC = {
        "name":        str,        # 唯一标识，用于 -q 参数
        "description": str,        # 人类可读描述
        "setup":       Callable,   # (spark, args) -> (sql, tag, n_rows, expected_out)
    }

setup() 职责：
  1. 自行生成所需 DataFrame（数据生成器自包含）
  2. 创建 UDF 并 spark.udf.register("process", ...)
  3. 注册临时表
  4. 返回 (sql_string, display_tag, input_row_count, expected_output_count)
     expected_output_count = -1 表示不确定
"""

import importlib
import pkgutil
from typing import Dict

_REGISTRY: Dict[str, dict] = {}


def discover(package_name: str = "udfs"):
    pkg = importlib.import_module(package_name)
    for _, mod_name, _ in pkgutil.iter_modules(pkg.__path__):
        module = importlib.import_module(f"{package_name}.{mod_name}")
        spec = getattr(module, "UDF_SPEC", None)
        if spec is not None:
            _REGISTRY[spec["name"]] = spec


def get(name: str) -> dict:
    if not _REGISTRY:
        discover()
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise KeyError(f"Unknown UDF '{name}'. Available: {available}")
    return _REGISTRY[name]


def list_all() -> list:
    if not _REGISTRY:
        discover()
    return sorted(_REGISTRY.keys())