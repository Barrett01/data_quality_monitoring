"""MySQL 存储层：封装连接管理和基础 SQL 操作。"""

from __future__ import annotations

import threading

import pymysql
from pymysql.cursors import DictCursor

from config.logger_config import get_logger
from config.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_CHARSET

logger = get_logger("SYSTEM", "MySQL")


class MySQLStorage:
    """MySQL 存储层，提供连接管理和基础 SQL 执行。

    使用 threading.local 实现线程安全的连接管理，
    每个 APScheduler 工作线程拥有独立的数据库连接。
    """

    def __init__(self):
        self._local = threading.local()

    def _get_connection(self):
        """获取当前线程的 MySQL 连接，带健康检查和重试。"""
        conn = getattr(self._local, "connection", None)

        # 健康检查：ping 检测连接是否存活
        if conn and conn.open:
            try:
                conn.ping(reconnect=True)
                return conn
            except pymysql.Error:
                logger.warning("MySQL 连接 ping 失败，将重新创建连接")
                conn = None

        # 创建新连接，带重试
        for attempt in range(3):
            try:
                conn = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE,
                    charset=MYSQL_CHARSET,
                    cursorclass=DictCursor,
                )
                self._local.connection = conn
                return conn
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

    def execute_batch(self, sql: str, params: list[tuple]) -> int:
        """批量执行更新/插入/删除，使用 executemany 提升性能。"""
        if not params:
            return 0
        conn = self._get_connection()
        with conn.cursor() as cursor:
            affected = cursor.executemany(sql, params)
            conn.commit()
            return affected

    def execute_in_transaction(self, callback) -> any:
        """在同一个事务中执行多步数据库操作，保证原子性。

        Args:
            callback: 接收 connection 对象的可调用对象，
                      在回调中通过 cursor 执行 SQL，无需手动 commit/rollback。

        Returns:
            callback 的返回值。

        Raises:
            callback 中抛出的任何异常都会触发 rollback。
        """
        conn = self._get_connection()
        try:
            result = callback(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise

    def close(self):
        """关闭当前线程的连接。"""
        conn = getattr(self._local, "connection", None)
        if conn and conn.open:
            conn.close()
            self._local.connection = None
            logger.info("MySQL 连接已关闭")
