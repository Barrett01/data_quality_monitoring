"""检查器抽象基类：定义检查流程的模板方法。"""

from abc import ABC, abstractmethod
from datetime import date, datetime

from config.logger_config import get_logger


class BaseChecker(ABC):
    """检查器基类，使用模板方法模式定义检查流程。"""

    def __init__(self, monitor_id: str, dimension: str):
        self.monitor_id = monitor_id
        self.dimension = dimension

    def execute(self, check_date: date, check_time: datetime, check_round: int):
        """执行检查的模板方法。"""
        log = get_logger(self.monitor_id, self.dimension)
        log.info(f"开始{self.dimension}检查 | check_date={check_date}, check_round={check_round}")
        try:
            self._prepare(check_date, check_time, check_round)
            result = self._check(check_date, check_time, check_round)
            self._record(check_date, check_time, check_round, result)
            self._alert(check_date, check_time, check_round, result)
        except Exception as e:
            log.critical(f"检查异常: {e}")
            # 将异常记录为 ERROR 状态到数据库
            try:
                self._record_error(check_date, check_time, check_round, str(e))
            except Exception as record_err:
                log.critical(f"记录ERROR状态失败: {record_err}")
            raise

    def _record_error(self, check_date: date, check_time: datetime, check_round: int, error_msg: str):
        """异常时记录 ERROR 状态到 dqm_check_result。子类必须实现此方法。"""
        raise NotImplementedError("子类必须实现 _record_error 方法以支持异常状态记录")

    @abstractmethod
    def _prepare(self, check_date: date, check_time: datetime, check_round: int):
        """准备阶段：采集数据等。"""
        pass

    @abstractmethod
    def _check(self, check_date: date, check_time: datetime, check_round: int) -> dict:
        """检查阶段：执行核心校验逻辑，返回检查结果。"""
        pass

    @abstractmethod
    def _record(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """记录阶段：将检查结果持久化到 MySQL。"""
        pass

    @abstractmethod
    def _alert(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """告警阶段：根据检查结果输出日志告警。"""
        pass
