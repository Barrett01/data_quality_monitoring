"""调度引擎：基于 APScheduler 实现定时任务调度。"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.logger_config import get_logger

logger = get_logger("SYSTEM", "调度")


class DQMScheduler:
    """数据质量监控调度引擎"""

    def __init__(self):
        self._scheduler = BlockingScheduler(timezone="Asia/Shanghai")
        self._jobs = {}

    def add_job(self, job_id: str, func, cron_expression: str, **kwargs):
        """添加定时任务。

        Args:
            job_id: 任务 ID
            func: 任务执行函数
            cron_expression: cron 表达式，如 "0 9 * * *"
        """
        parts = cron_expression.split()
        trigger = CronTrigger(
            minute=parts[0] if len(parts) > 0 else "*",
            hour=parts[1] if len(parts) > 1 else "*",
            day=parts[2] if len(parts) > 2 else "*",
            month=parts[3] if len(parts) > 3 else "*",
            day_of_week=parts[4] if len(parts) > 4 else "*",
            timezone="Asia/Shanghai",
        )
        self._scheduler.add_job(func, trigger, id=job_id, kwargs=kwargs, misfire_grace_time=30)
        self._jobs[job_id] = cron_expression
        logger.info(f"注册定时任务: {job_id} -> {cron_expression}")

    def start(self):
        """启动调度器。"""
        logger.info("调度引擎启动")
        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("调度引擎停止")
            self._scheduler.shutdown()

    def get_jobs_info(self) -> dict:
        """获取所有注册任务信息。"""
        return dict(self._jobs)
