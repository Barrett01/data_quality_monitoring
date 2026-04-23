"""检查器抽象基类：定义检查流程的模板方法。"""

import signal
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import date, datetime

from config.settings import CHECK_TIMEOUT_SECONDS
from config.logger_config import get_logger


class CheckTimeoutError(Exception):
    """检查超时异常。"""
    pass


class BaseChecker(ABC):
    """检查器基类，使用模板方法模式定义检查流程。

    支持跨平台超时控制：
    - Unix/Linux: 优先使用 signal.SIGALRM（精确、低开销）
    - Windows/通用: 使用 ThreadPoolExecutor + 超时等待（兼容所有平台）
    """

    def __init__(self, monitor_id: str, dimension: str):
        self.monitor_id = monitor_id
        self.dimension = dimension

    def execute(self, check_date: date, check_time: datetime, check_round: int):
        """执行检查的模板方法，带超时控制。"""
        log = get_logger(self.monitor_id, self.dimension)
        log.info(f"开始{self.dimension}检查 | check_date={check_date}, check_round={check_round}")

        # 构造超时错误信息
        timeout_msg = (
            f"检查超时 ({CHECK_TIMEOUT_SECONDS}s) | "
            f"monitor_id={self.monitor_id}, check_date={check_date}, check_round={check_round}"
        )

        # ── 策略 1: Unix/Linux 使用 SIGALRM（精确，低开销） ──
        timeout_set = False
        old_handler = None
        try:
            old_handler = signal.getsignal(signal.SIGALRM)

            def _timeout_handler(signum, frame):
                raise CheckTimeoutError(timeout_msg)

            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(CHECK_TIMEOUT_SECONDS)
            timeout_set = True
        except (AttributeError, OSError, ValueError):
            # Windows 不支持 SIGALRM，或不在主线程中，使用策略 2
            pass

        if timeout_set:
            # Unix: 使用 SIGALRM 超时，直接在主线程执行
            try:
                self._prepare(check_date, check_time, check_round)
                result = self._check(check_date, check_time, check_round)
                self._record(check_date, check_time, check_round, result)
                self._alert(check_date, check_time, check_round, result)
            except CheckTimeoutError as e:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
                log.warning(str(e))
                try:
                    self._record_error(check_date, check_time, check_round, str(e))
                except Exception as record_err:
                    log.critical(f"记录超时ERROR状态失败: {record_err}")
                raise
            except Exception as e:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
                log.critical(f"检查异常: {e}")
                try:
                    self._record_error(check_date, check_time, check_round, str(e))
                except Exception as record_err:
                    log.critical(f"记录ERROR状态失败: {record_err}")
                raise
            else:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        else:
            # ── 策略 2: 通用平台使用 ThreadPoolExecutor 超时 ──
            # 只在线程内运行 _prepare 和 _check（耗时操作），
            # _record 和 _alert 在主线程执行，避免超时后被工作线程覆盖。
            execution_result = {}
            execution_error = {}

            def _run_core():
                """在子线程中执行采集和检查（耗时操作）。"""
                try:
                    self._prepare(check_date, check_time, check_round)
                    result = self._check(check_date, check_time, check_round)
                    execution_result["data"] = result
                except Exception as e:
                    execution_error["exception"] = e

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_core)
                try:
                    future.result(timeout=CHECK_TIMEOUT_SECONDS)
                except FuturesTimeoutError:
                    timeout_err = CheckTimeoutError(timeout_msg)
                    log.warning(str(timeout_err))
                    # 工作线程仍在运行但不再等待，_record/_alert 不会被执行
                    try:
                        self._record_error(check_date, check_time, check_round, str(timeout_err))
                    except Exception as record_err:
                        log.critical(f"记录超时ERROR状态失败: {record_err}")
                    raise timeout_err

            # 线程正常完成，在主线程中执行记录和告警
            if "exception" in execution_error:
                real_exc = execution_error["exception"]
                log.critical(f"检查异常: {real_exc}")
                try:
                    self._record_error(check_date, check_time, check_round, str(real_exc))
                except Exception as record_err:
                    log.critical(f"记录ERROR状态失败: {record_err}")
                raise real_exc

            result = execution_result["data"]
            self._record(check_date, check_time, check_round, result)
            self._alert(check_date, check_time, check_round, result)

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
