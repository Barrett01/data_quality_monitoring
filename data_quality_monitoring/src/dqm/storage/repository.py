"""数据访问层：封装常用数据库操作。"""

from __future__ import annotations

from datetime import date

from src.dqm.storage.mysql_storage import MySQLStorage
from config.logger_config import get_logger

logger = get_logger("SYSTEM", "Repository")


class CheckResultRepository:
    """检查结果数据访问"""

    def __init__(self, storage: MySQLStorage):
        self._storage = storage

    def upsert(self, check_date: date, check_round: int, monitor_id: str, dimension: str, result: str, detail: str = ""):
        """插入或更新检查结果。"""
        sql = """
        INSERT INTO dqm_check_result (check_date, check_round, check_time, monitor_id, dimension, result, detail)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            check_time = NOW(), result = VALUES(result), detail = VALUES(detail), updated_at = NOW()
        """
        self._storage.execute_update(sql, (check_date, check_round, monitor_id, dimension, result, detail))

    def get_by_date(self, check_date: date, monitor_id: str | None = None) -> list[dict]:
        """按日期查询检查结果。"""
        sql = "SELECT * FROM dqm_check_result WHERE check_date = %s"
        params = [check_date]
        if monitor_id:
            sql += " AND monitor_id = %s"
            params.append(monitor_id)
        return self._storage.execute_query(sql, tuple(params))

    def cleanup(self, retention_days: int) -> int:
        """清理过期检查结果。"""
        sql = "DELETE FROM dqm_check_result WHERE check_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)"
        return self._storage.execute_update(sql, (retention_days,))


class SnapshotRepository:
    """快照数据访问"""

    def __init__(self, storage: MySQLStorage):
        self._storage = storage

    def save(self, check_date: date, check_round: int, records: list[dict]):
        """保存快照数据（事务内先删后插，保证原子性）。"""
        sql_insert = """
        INSERT INTO dqm_security_info_snapshot
        (check_date, check_round, check_time, stkcode, stkname, std_stkcode, mst_type,
         compn_stock_code, compn_stock_name, index_name, send_date)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_params = [
            (
                check_date,
                check_round,
                r.get("stkcode"),
                r.get("stkname"),
                r.get("std_stkcode"),
                r.get("mst_type"),
                r.get("compn_stock_code"),
                r.get("compn_stock_name"),
                r.get("index_name"),
                r.get("send_date"),
            )
            for r in records
        ]

        def _tx(conn):
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dqm_security_info_snapshot WHERE check_date = %s AND check_round = %s",
                    (check_date, check_round),
                )
                if insert_params:
                    cursor.executemany(sql_insert, insert_params)

        self._storage.execute_in_transaction(_tx)

    def get_by_date(self, check_date: date, check_round: int | None = None) -> list[dict]:
        """查询快照数据。"""
        sql = "SELECT * FROM dqm_security_info_snapshot WHERE check_date = %s"
        params = [check_date]
        if check_round is not None:
            sql += " AND check_round = %s"
            params.append(check_round)
        return self._storage.execute_query(sql, tuple(params))

    def cleanup(self, retention_days: int) -> int:
        """清理过期快照。"""
        sql = "DELETE FROM dqm_security_info_snapshot WHERE check_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)"
        return self._storage.execute_update(sql, (retention_days,))


class AccuracyDetailRepository:
    """准确性明细数据访问"""

    def __init__(self, storage: MySQLStorage):
        self._storage = storage

    def save_batch(self, check_date: date, check_round: int, monitor_id: str, details: list[dict]):
        """批量保存准确性明细（事务内先删后插，保证原子性和幂等性）。"""
        sql_insert = """
        INSERT INTO dqm_accuracy_detail (check_date, check_round, monitor_id, record_key, field_name, error_type, error_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        insert_params = [
            (
                check_date,
                check_round,
                monitor_id,
                d.get("record_key"),
                d.get("field_name"),
                d.get("error_type"),
                str(d.get("error_value", "")),
            )
            for d in details
        ]

        def _tx(conn):
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dqm_accuracy_detail WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
                    (check_date, check_round, monitor_id),
                )
                if insert_params:
                    cursor.executemany(sql_insert, insert_params)

        self._storage.execute_in_transaction(_tx)

    def cleanup(self, retention_days: int) -> int:
        """清理过期明细。"""
        sql = "DELETE FROM dqm_accuracy_detail WHERE check_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)"
        return self._storage.execute_update(sql, (retention_days,))
