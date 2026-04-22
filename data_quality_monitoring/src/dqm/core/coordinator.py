"""检查协调器：编排各检查器的执行流程。"""

from __future__ import annotations

from datetime import date, datetime

from config.logger_config import get_logger

logger = get_logger("SYSTEM", "协调")


class Coordinator:
    """检查协调器，负责按序调度各监控项的检查流程。"""

    def __init__(self):
        self._checkers = {}

    def register(self, monitor_id: str, checker):
        """注册检查器。"""
        self._checkers[monitor_id] = checker
        logger.info(f"注册检查器: {monitor_id}")

    def run_check(self, monitor_id: str, check_round: int = 1):
        """执行指定监控项的检查。

        Args:
            monitor_id: 监控项 ID（M1-M6）
            check_round: 当天第几轮检查
        """
        checker = self._checkers.get(monitor_id)
        if not checker:
            logger.error(f"未找到检查器: {monitor_id}")
            return

        check_date = date.today()
        check_time = datetime.now()
        log = get_logger(monitor_id, checker.dimension)

        log.info(f"开始检查: check_date={check_date}, check_round={check_round}")
        try:
            checker.execute(check_date=check_date, check_time=check_time, check_round=check_round)
        except Exception as e:
            log.critical(f"检查异常终止: {e}")
        else:
            log.info(f"检查完成: check_date={check_date}, check_round={check_round}")

    def run_all(self, monitor_ids: list[str] | None = None, check_round: int = 1):
        """批量执行检查。"""
        ids = monitor_ids or list(self._checkers.keys())
        for mid in ids:
            self.run_check(mid, check_round)
