"""类型校验器：校验字段数据类型。（未启用——当前 AccuracyChecker 使用内部校验逻辑）"""

from __future__ import annotations

from typing import Any


class TypeValidator:
    """类型校验器"""

    @staticmethod
    def is_string(value: Any) -> bool:
        return isinstance(value, str)

    @staticmethod
    def is_integer(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return isinstance(value, int)

    @staticmethod
    def is_float(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return isinstance(value, (int, float))

    @staticmethod
    def is_date_string(value: Any, formats: list[str] | None = None) -> bool:
        """校验字符串是否匹配日期格式。"""
        if not isinstance(value, str):
            return False
        formats = formats or [r"^\d{4}-\d{2}-\d{2}$", r"^\d{8}$"]
        import re
        return any(re.match(f, value) for f in formats)
