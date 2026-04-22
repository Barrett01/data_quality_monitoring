#!/usr/bin/env python
"""服务状态查询脚本。"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger


def check_status():
    """查询服务状态。"""
    setup_logger()
    logger = get_logger("SYSTEM", "状态")

    pid_file = project_root / "dqm.pid"
    if not pid_file.exists():
        print("数据质量监控服务: 未运行")
        return

    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, 0)  # 检查进程是否存在
        print(f"数据质量监控服务: 运行中 (PID={pid})")
    except ProcessLookupError:
        print(f"数据质量监控服务: 异常 (PID={pid} 已不存在)")
        pid_file.unlink()


if __name__ == "__main__":
    check_status()
