"""MySQL 存储层：封装连接管理和基础 SQL 操作。

提供查询/写入重试机制和超时控制：
- 查询失败：最多重试 MYSQL_QUERY_RETRY 次，间隔 MYSQL_QUERY_RETRY_INTERVAL 秒
- 写入失败：最多重试 MYSQL_WRITE_RETRY 次，间隔 MYSQL_WRITE_RETRY_INTERVAL 秒
- 超时控制：单次操作超过 CHECK_TIMEOUT_SECONDS 秒则告警并终止
"""

from __future__ import annotations

import threading
import time

import pymysql
from pymysql.cursors import DictCursor

from config.constants import (
    MYSQL_QUERY_RETRY,
    MYSQL_QUERY_RETRY_INTERVAL,
    MYSQL_WRITE_RETRY,
    MYSQL_WRITE_RETRY_INTERVAL,
)
from config.logger_config import get_logger
from config.settings import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    MYSQL_CHARSET,
    CHECK_TIMEOUT_SECONDS,
)

logger = get_logger("SYSTEM", "MySQL")


class MySQLOperationTimeout(Exception):
    """MySQL 操作超时异常。"""
    pass


class MySQLStorage:
    """MySQL 存储层，提供连接管理和基础 SQL 执行。

    使用 threading.local 实现线程安全的连接管理，
    每个 APScheduler 工作线程拥有独立的数据库连接。

    重试机制：
    - execute_query: 查询失败后重试 MYSQL_QUERY_RETRY 次
    - execute_update / execute_batch: 写入失败后重试 MYSQL_WRITE_RETRY 次
    - 超时控制: 单次操作超过 CHECK_TIMEOUT_SECONDS 秒则告警并抛出异常
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

    def _execute_with_retry(self, operation_type: str, func, *args, **kwargs):
        """带重试和超时控制的执行包装器。

        Args:
            operation_type: "query" 或 "write"
            func: 实际执行的函数
            *args, **kwargs: 传给 func 的参数

        Returns:
            func 的返回值

        Raises:
            MySQLOperationTimeout: 操作超时
            pymysql.Error: 重试耗尽后仍失败
        """
        if operation_type == "query":
            max_retries = MYSQL_QUERY_RETRY
            retry_interval = MYSQL_QUERY_RETRY_INTERVAL
        else:
            max_retries = MYSQL_WRITE_RETRY
            retry_interval = MYSQL_WRITE_RETRY_INTERVAL

        start_time = time.time()
        last_error = None

        for attempt in range(1, max_retries + 1):
            # 超时检查
            elapsed = time.time() - start_time
            if elapsed > CHECK_TIMEOUT_SECONDS:
                logger.error(
                    f"MySQL {operation_type} 操作超时 | "
                    f"elapsed={elapsed:.1f}s, timeout={CHECK_TIMEOUT_SECONDS}s, "
                    f"attempt={attempt}/{max_retries}"
                )
                raise MySQLOperationTimeout(
                    f"MySQL {operation_type} 操作超时 ({elapsed:.1f}s > {CHECK_TIMEOUT_SECONDS}s)"
                )

            try:
                result = func(*args, **kwargs)
                return result
            except pymysql.Error as e:
                last_error = e
                elapsed = time.time() - start_time
                # 超时后不再重试
                if elapsed > CHECK_TIMEOUT_SECONDS:
                    logger.error(
                        f"MySQL {operation_type} 操作超时 | "
                        f"elapsed={elapsed:.1f}s, timeout={CHECK_TIMEOUT_SECONDS}s"
                    )
                    raise MySQLOperationTimeout(
                        f"MySQL {operation_type} 操作超时 ({elapsed:.1f}s > {CHECK_TIMEOUT_SECONDS}s)"
                    ) from e

                if attempt < max_retries:
                    logger.warning(
                        f"MySQL {operation_type} 失败(第{attempt}次)，"
                        f"{retry_interval}s 后重试 | error={e}"
                    )
                    time.sleep(retry_interval)
                else:
                    logger.error(
                        f"MySQL {operation_type} 重试耗尽({max_retries}次) | error={e}"
                    )
                    raise

        # 理论上不会走到这里，但作为保险
        raise last_error  # type: ignore[misc]

    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict]:
        """执行查询，带重试和超时控制。"""
        def _do_query():
            conn = self._get_connection()
            conn.commit()  # 刷新事务快照，确保读取其他连接最新已提交的数据
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

        return self._execute_with_retry("query", _do_query)

    def execute_update(self, sql: str, params: tuple | None = None) -> int:
        """执行更新/插入/删除，带重试和超时控制。"""
        def _do_update():
            conn = self._get_connection()
            with conn.cursor() as cursor:
                affected = cursor.execute(sql, params)
                conn.commit()
                return affected

        return self._execute_with_retry("write", _do_update)

    def execute_batch(self, sql: str, params: list[tuple]) -> int:
        """批量执行更新/插入/删除，带重试和超时控制。"""
        if not params:
            return 0

        def _do_batch():
            conn = self._get_connection()
            with conn.cursor() as cursor:
                affected = cursor.executemany(sql, params)
                conn.commit()
                return affected

        return self._execute_with_retry("write", _do_batch)

    def execute_in_transaction(self, callback) -> any:
        """在同一个事务中执行多步数据库操作，保证原子性。带重试和超时控制。

        Args:
            callback: 接收 connection 对象的可调用对象，
                      在回调中通过 cursor 执行 SQL，无需手动 commit/rollback。

        Returns:
            callback 的返回值。

        Raises:
            callback 中抛出的任何异常都会触发 rollback。
            MySQLOperationTimeout: 操作超时。
        """
        def _do_transaction():
            conn = self._get_connection()
            try:
                result = callback(conn)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

        return self._execute_with_retry("write", _do_transaction)

    def close(self):
        """关闭当前线程的连接。"""
        conn = getattr(self._local, "connection", None)
        if conn and conn.open:
            conn.close()
            self._local.connection = None
            logger.info("MySQL 连接已关闭")
