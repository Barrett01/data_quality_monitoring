"""准确性检查器：校验字段非空和类型约束。"""

from datetime import date, datetime

from src.dqm.checkers.base import BaseChecker
from config.constants import Dimension


class AccuracyChecker(BaseChecker):
    """准确性检查器"""

    def __init__(self, monitor_id: str):
        super().__init__(monitor_id, Dimension.ACCURACY)

    def _prepare(self, check_date: date, check_time: datetime, check_round: int):
        # TODO: 查询线上表数据
        pass

    def _check(self, check_date: date, check_time: datetime, check_round: int) -> dict:
        # TODO: 校验字段非空和类型
        return {"status": "PASS", "total": 0, "errors": 0, "details": []}

    def _record(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        # TODO: 写入 dqm_check_result 和 dqm_accuracy_detail
        pass

    def _alert(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        # TODO: 输出告警日志
        pass
