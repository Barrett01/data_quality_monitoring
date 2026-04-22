"""日志配置：基于 loguru，支持控制台 + 文件输出。"""

import sys
from pathlib import Path

from loguru import logger

from config.settings import LOG_LEVEL, LOG_DIR


def setup_logger() -> None:
    """初始化日志配置。"""
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 移除默认 handler
    logger.remove()

    # 控制台输出
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[monitor_id]}</cyan> | "
            "<cyan>{extra[dimension]}</cyan> | "
            "<level>{message}</level>"
        ),
        enqueue=True,
    )

    # 主日志文件
    logger.add(
        log_dir / "dqm.log",
        level=LOG_LEVEL,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[monitor_id]} | "
            "{extra[dimension]} | {message}"
        ),
        rotation="10 MB",
        retention="30 days",
        encoding="utf-8",
        enqueue=True,
    )

    # 错误日志文件
    logger.add(
        log_dir / "dqm_error.log",
        level="ERROR",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[monitor_id]} | "
            "{extra[dimension]} | {message}"
        ),
        rotation="10 MB",
        retention="30 days",
        encoding="utf-8",
        enqueue=True,
    )

    # 告警日志文件
    logger.add(
        log_dir / "dqm_alert.log",
        level="WARNING",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[monitor_id]} | "
            "{extra[dimension]} | {message}"
        ),
        rotation="10 MB",
        retention="30 days",
        encoding="utf-8",
        enqueue=True,
    )

    # 设置默认 extra
    logger.configure(extra={"monitor_id": "SYSTEM", "dimension": "-"})


def get_logger(monitor_id: str = "SYSTEM", dimension: str = "-"):
    """获取绑定 monitor_id 和 dimension 的 logger。"""
    return logger.bind(monitor_id=monitor_id, dimension=dimension)
