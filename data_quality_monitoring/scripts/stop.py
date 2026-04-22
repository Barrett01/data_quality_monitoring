#!/usr/bin/env python
"""服务停止脚本。"""

import os
import signal
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger


def stop_service():
    """停止监控服务。"""
    setup_logger()
    logger = get_logger("SYSTEM", "停止")

    pid_file = project_root / "dqm.pid"
    if not pid_file.exists():
        logger.warning("未找到 PID 文件，服务可能未运行")
        return

    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        logger.info(f"已发送停止信号: PID={pid}")
        pid_file.unlink()
    except ProcessLookupError:
        logger.warning(f"进程不存在: PID={pid}")
        pid_file.unlink()


if __name__ == "__main__":
    stop_service()
