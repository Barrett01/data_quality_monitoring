"""MongoDB 数据采集器：查询线上表数据。"""

from __future__ import annotations

from datetime import date

from pymongo import MongoClient

from config.logger_config import get_logger
from config.settings import MONGO_URI, MONGO_DATABASE
logger = get_logger("SYSTEM", "MongoDB")


class MongoCollector:
    """MongoDB 数据采集器"""

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
