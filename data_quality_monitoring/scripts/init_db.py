#!/usr/bin/env python
"""数据库初始化脚本：创建监控所需的数据表。"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL


def init_database():
    """初始化数据库表。"""
    setup_logger()
    logger = get_logger("SYSTEM", "初始化")

    storage = MySQLStorage()
    try:
        for ddl in ALL_DDL:
            storage.execute_update(ddl)
            logger.info(f"数据表创建成功")
        logger.info("数据库初始化完成")
    except Exception as e:
        logger.critical(f"数据库初始化失败: {e}")
        raise
    finally:
        storage.close()


if __name__ == "__main__":
    init_database()
