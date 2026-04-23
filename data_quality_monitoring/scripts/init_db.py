#!/usr/bin/env python
"""数据库初始化脚本：创建监控所需的数据表。"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_logger, get_logger
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL

# 增量迁移：为已存在的 dqm_accuracy_detail 表添加唯一键
_MIGRATIONS = [
    {
        "check": "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dqm_accuracy_detail' AND INDEX_NAME = 'uk_detail'",
        "sql": "ALTER TABLE `dqm_accuracy_detail` ADD UNIQUE KEY `uk_detail` (`check_date`, `check_round`, `monitor_id`, `record_key`, `field_name`)",
        "desc": "dqm_accuracy_detail 添加唯一键 uk_detail",
    },
]


def init_database():
    """初始化数据库表。"""
    setup_logger()
    logger = get_logger("SYSTEM", "初始化")

    storage = MySQLStorage()
    try:
        for ddl in ALL_DDL:
            storage.execute_update(ddl)
            logger.info("数据表创建成功")

        # 执行增量迁移
        for mig in _MIGRATIONS:
            try:
                rows = storage.execute_query(mig["check"])
                if rows[0]["COUNT(*)"] == 0:
                    storage.execute_update(mig["sql"])
                    logger.info(f"迁移成功: {mig['desc']}")
                else:
                    logger.info(f"迁移跳过(已存在): {mig['desc']}")
            except Exception as e:
                logger.warning(f"迁移失败(可忽略): {mig['desc']} | error={e}")

        logger.info("数据库初始化完成")
    except Exception as e:
        logger.critical(f"数据库初始化失败: {e}")
        raise
    finally:
        storage.close()


if __name__ == "__main__":
    init_database()
