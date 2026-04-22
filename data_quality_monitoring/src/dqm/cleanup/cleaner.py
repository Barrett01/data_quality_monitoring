"""过期数据清理器：清理超期的快照、检查结果和明细数据。"""

from __future__ import annotations

from config.logger_config import get_logger
from config.settings import SNAPSHOT_RETENTION_DAYS, CHECK_RESULT_RETENTION_DAYS, ACCURACY_DETAIL_RETENTION_DAYS
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import (
    AccuracyDetailRepository,
    CheckResultRepository,
    SnapshotRepository,
)

logger = get_logger("SYSTEM", "清理")


class DataCleaner:
    """过期数据清理器"""

    def __init__(self, storage: MySQLStorage):
        self._snapshot_repo = SnapshotRepository(storage)
        self._check_result_repo = CheckResultRepository(storage)
        self._accuracy_detail_repo = AccuracyDetailRepository(storage)

    def run(self):
        """执行清理。"""
        logger.info("开始过期数据清理...")

        try:
            deleted = self._snapshot_repo.cleanup(SNAPSHOT_RETENTION_DAYS)
            logger.info(f"快照清理完成: deleted={deleted}, retention={SNAPSHOT_RETENTION_DAYS}天")
        except Exception as e:
            logger.error(f"快照清理失败: {e}")

        try:
            deleted = self._check_result_repo.cleanup(CHECK_RESULT_RETENTION_DAYS)
            logger.info(f"检查结果清理完成: deleted={deleted}, retention={CHECK_RESULT_RETENTION_DAYS}天")
        except Exception as e:
            logger.error(f"检查结果清理失败: {e}")

        try:
            deleted = self._accuracy_detail_repo.cleanup(ACCURACY_DETAIL_RETENTION_DAYS)
            logger.info(f"准确性明细清理完成: deleted={deleted}, retention={ACCURACY_DETAIL_RETENTION_DAYS}天")
        except Exception as e:
            logger.error(f"准确性明细失败: {e}")

        logger.info("过期数据清理结束")
