#!/usr/bin/env python
"""手动执行检查脚本：支持指定监控项和检查轮次。"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger
from src.dqm.core.runner import create_app


def main():
    setup_logger()
    logger = get_logger("SYSTEM", "手动检查")

    parser = argparse.ArgumentParser(description="手动执行数据质量检查")
    parser.add_argument("--monitor", "-m", type=str, default="all", help="监控项ID: M1-M6 或 all")
    parser.add_argument("--round", "-r", type=int, default=1, help="检查轮次")
    args = parser.parse_args()

    _, coordinator = create_app()

    if args.monitor == "all":
        coordinator.run_all(check_round=args.round)
    else:
        coordinator.run_check(args.monitor, check_round=args.round)

    logger.info(f"手动检查完成: monitor={args.monitor}, round={args.round}")


if __name__ == "__main__":
    main()
