"""MySQL 存储层：封装连接管理和基础 SQL 操作。"""

from __future__ import annotations

import pymysql
from pymysql.cursors import DictCursor

from config.logger_config import get_logger
from config.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_CHARSET

logger = get_logger("SYSTEM", "MySQL")


class MySQLStorage:
    """MySQL 存储层，提供连接管理和基础 SQL 执行。"""

    def __init__(self):
        self._connection = None

    def _get_connection(self):
        """获取 MySQL 连接，带重试。"""
        if self._connection and self._connection.open:
            return self._connection

        for attempt in range(3):
            try:
                self._connection = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE,
                    charset=MYSQL_CHARSET,
                    cursorclass=DictCursor,
                )
                return self._connection
            except pymysql.Error as e:
                logger.warning(f"MySQL 连接失败(第{attempt + 1}次): {e}")
                if attempt == 2:
                    raise
        return None

    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict]:
        """执行查询。"""
        conn = self._get_connection()
        conn.commit()  # 刷新事务快照，确保读取其他连接最新已提交的数据
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def execute_update(self, sql: str, params: tuple | None = None) -> int:
        """执行更新/插入/删除。"""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            affected = cursor.execute(sql, params)
            conn.commit()
            return affected

    def close(self):
        """关闭连接。"""
        if self._connection and self._connection.open:
            self._connection.close()
            logger.info("MySQL 连接已关闭")
