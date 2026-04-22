"""运行入口：初始化各组件并启动调度引擎。"""

from __future__ import annotations

from config.constants import Dimension
from config.logger_config import setup_logger, get_logger
from config.monitor_configs import ALL_CONFIGS
from src.dqm.checkers.accuracy import AccuracyChecker
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.checkers.timeliness import TimelinessChecker
from src.dqm.core.coordinator import Coordinator
from src.dqm.core.scheduler import DQMScheduler
from src.dqm.storage.mysql_storage import MySQLStorage

logger = get_logger("SYSTEM", "启动")


def create_app() -> tuple[DQMScheduler, Coordinator]:
    """创建应用实例，初始化调度器和协调器。"""
    setup_logger()
    logger.info("初始化数据质量监控系统...")

    scheduler = DQMScheduler()
    coordinator = Coordinator()
    mysql_storage = MySQLStorage()

    # 根据 ALL_CONFIGS 注册检查器
    for monitor_id, cfg in ALL_CONFIGS.items():
        if cfg["dimension"] == Dimension.COMPLETENESS:
            checker = CompletenessChecker(
                monitor_id=cfg["monitor_id"],
                table=cfg["table"],
                key_field=cfg["key_field"],
                mysql_storage=mysql_storage,
                group_field=cfg.get("group_field"),
            )
            coordinator.register(monitor_id, checker)
        elif cfg["dimension"] == Dimension.TIMELINESS:
            checker = TimelinessChecker(
                monitor_id=cfg["monitor_id"],
                table=cfg["table"],
                mysql_storage=mysql_storage,
            )
            coordinator.register(monitor_id, checker)
        elif cfg["dimension"] == Dimension.ACCURACY:
            checker = AccuracyChecker(
                monitor_id=cfg["monitor_id"],
                table=cfg["table"],
                key_field=cfg["key_field"],
                mysql_storage=mysql_storage,
            )
            coordinator.register(monitor_id, checker)

        # 注册定时任务
        for idx, time_str in enumerate(cfg["cron_times"], start=1):
            hour, minute = time_str.split(":")
            scheduler.add_job(
                job_id=f"{monitor_id}_R{idx}",
                func=coordinator.run_check,
                cron_expression=f"{minute} {hour} * * *",
                monitor_id=monitor_id,
                check_round=idx,
            )

    logger.info("数据质量监控系统初始化完成")
    return scheduler, coordinator


def run():
    """启动监控服务。"""
    scheduler, coordinator = create_app()
    logger.info("启动调度引擎，开始监控...")
    scheduler.start()


if __name__ == "__main__":
    run()
