#!/usr/bin/env python
"""服务启动脚本。"""

import os
import sys
from pathlib import Path

# 将项目根目录加入 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger


def run():
    """启动监控服务。"""
    setup_logger()
    logger = get_logger("SYSTEM", "启动")

    from src.dqm.core.runner import create_app

    scheduler, coordinator = create_app()

    # 创建 PID 文件，供 stop.py / status.py 使用
    pid_file = project_root / "dqm.pid"
    pid_file.write_text(str(os.getpid()))
    logger.info(f"PID 文件已创建: {pid_file} (PID={os.getpid()})")

    try:
        logger.info("启动调度引擎，开始监控...")
        scheduler.start()
    finally:
        # 服务退出时清理 PID 文件
        if pid_file.exists():
            pid_file.unlink()
            logger.info("PID 文件已清理")


if __name__ == "__main__":
    run()
