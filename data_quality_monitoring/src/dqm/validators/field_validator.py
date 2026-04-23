"""字段校验器：校验字段非空和值约束。（未启用——当前 AccuracyChecker 使用内部校验逻辑）"""

from __future__ import annotations

import re
from typing import Any


class FieldValidator:
    """字段校验器"""

    @staticmethod
    def is_not_empty(value: Any) -> bool:
        """校验字段非空。"""
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    @staticmethod
    def match_regex(value: str, pattern: str) -> bool:
        """校验字段值匹配正则。"""
        if not isinstance(value, str):
            return False
        return bool(re.match(pattern, value))

    @staticmethod
    def is_enum(value: str, allowed: set[str]) -> bool:
        """校验字段值为枚举之一。"""
        return value in allowed

    @staticmethod
    def is_number(value: Any, min_val: float | None = None) -> bool:
        """校验字段值为数值类型。"""
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            if min_val is not None and value < min_val:
                return False
            return True
        return False
