"""MongoDB 数据采集器：查询线上表数据。

暂不使用 — 当前线上表均在 MySQL 中，未来可能切换到 MongoDB 时启用。
"""

from __future__ import annotations

from datetime import date

# NOTE: pymongo 暂未使用，保留依赖以备未来切换 MongoDB
from pymongo import MongoClient  # noqa: F401

from config.logger_config import get_logger
from config.settings import MONGO_URI, MONGO_DATABASE
logger = get_logger("SYSTEM", "MongoDB")


class MongoCollector:
    """MongoDB 数据采集器（暂不使用）

    当前线上表均在 MySQL 中。若未来线上表迁移至 MongoDB，
    需同步修改 CompletenessChecker / TimelinessChecker / AccuracyChecker
    中的数据查询逻辑，将 MySQL 查询替换为 MongoCollector.collect()。
    """

    def __init__(self):
        self._client: MongoClient | None = None
        self._db = None

    def _ensure_connection(self):
        if self._client is None:
            self._client = MongoClient(MONGO_URI)
            self._db = self._client[MONGO_DATABASE]
            logger.info("MongoDB 连接已建立")

    def collect(self, check_date: date, collection: str = "", query: dict | None = None, **kwargs) -> list[dict]:
        """从 MongoDB 查询数据。"""
        self._ensure_connection()
        if query is None:
            query = {}
        cursor = self._db[collection].find(query, {"_id": 0})
        results = list(cursor)
        logger.info(f"MongoDB 查询完成: collection={collection}, count={len(results)}")
        return results

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB 连接已关闭")
