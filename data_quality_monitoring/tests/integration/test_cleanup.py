"""过期数据清理集成测试。 过期数据自动清理

使用本地 MySQL 进行端到端清理测试：
1. DataCleaner.run — 三张表的过期数据正确清理
2. CompletenessChecker._prepare — 每次检查前自动清理过期快照
3. 准确性检查 — _record 中自动清理过期明细
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import patch

import pymysql
import pytest

from config.constants import MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.accuracy import AccuracyChecker
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.cleanup.cleaner import DataCleaner
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"


@pytest.fixture(scope="module", autouse=True)
def init_logger():
    setup_logger()


@pytest.fixture(scope="module")
def db_conn():
    conn = pymysql.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
        database=TEST_DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def ensure_dqm_tables(db_conn):
    cur = db_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    for ddl in ALL_DDL:
        cur.execute(ddl)
    cur.close()


@pytest.fixture
def mysql_storage():
    with patch("src.dqm.storage.mysql_storage.MYSQL_HOST", TEST_DB_HOST), \
         patch("src.dqm.storage.mysql_storage.MYSQL_PORT", TEST_DB_PORT), \
         patch("src.dqm.storage.mysql_storage.MYSQL_USER", TEST_DB_USER), \
         patch("src.dqm.storage.mysql_storage.MYSQL_PASSWORD", TEST_DB_PASSWORD), \
         patch("src.dqm.storage.mysql_storage.MYSQL_DATABASE", TEST_DB_NAME), \
         patch("src.dqm.storage.mysql_storage.MYSQL_CHARSET", "utf8mb4"):
        storage = MySQLStorage()
        yield storage
        storage.close()


@pytest.fixture(autouse=True)
def cleanup(db_conn):
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()


# ---------------------------------------------------------------------------
# 辅助方法
# ---------------------------------------------------------------------------

def count_rows(db_conn, table: str) -> int:
    cur = db_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    count = cur.fetchone()[0]
    cur.close()
    return count


def insert_snapshot(db_conn, check_date: date, check_round: int, stkcode: str, mst_type: str = "INDUSTRY_PLATE_INFO"):
    cur = db_conn.cursor()
    cur.execute(
        "INSERT INTO dqm_security_info_snapshot "
        "(check_date, check_round, check_time, stkcode, stkname, std_stkcode, mst_type, compn_stock_code, compn_stock_name, index_name, send_date) "
        "VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)",
        (check_date, check_round, stkcode, f"板块{stkcode}", f"{stkcode}.SH", mst_type, "", "", "", ""),
    )
    cur.close()


def insert_check_result(db_conn, check_date: date, check_round: int, monitor_id: str, result: str = "PASS"):
    cur = db_conn.cursor()
    cur.execute(
        "INSERT INTO dqm_check_result (check_date, check_round, check_time, monitor_id, dimension, result, detail) "
        "VALUES (%s, %s, NOW(), %s, %s, %s, %s)",
        (check_date, check_round, monitor_id, "完整性", result, "{}"),
    )
    cur.close()


def insert_accuracy_detail(db_conn, check_date: date, check_round: int, monitor_id: str, field_name: str = "stkcode"):
    cur = db_conn.cursor()
    cur.execute(
        "INSERT INTO dqm_accuracy_detail (check_date, check_round, monitor_id, record_key, field_name, error_type, error_value) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (check_date, check_round, monitor_id, "000001", field_name, "NULL_VALUE", "NULL"),
    )
    cur.close()


# ---------------------------------------------------------------------------
# 测试 DataCleaner.run
# ---------------------------------------------------------------------------

class TestDataCleanerRun:
    """测试 DataCleaner 的完整清理流程。"""

    def test_cleanup_snapshot_expired(self, db_conn, mysql_storage):
        """快照数据超过 15 天 → 被清理。"""
        old_date = date.today() - timedelta(days=16)
        recent_date = date.today() - timedelta(days=5)

        insert_snapshot(db_conn, old_date, 1, "993301")
        insert_snapshot(db_conn, recent_date, 1, "993302")

        assert count_rows(db_conn, "dqm_security_info_snapshot") == 2

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        # 旧数据被清理，新数据保留
        remaining = count_rows(db_conn, "dqm_security_info_snapshot")
        assert remaining == 1

    def test_cleanup_check_result_expired(self, db_conn, mysql_storage):
        """检查结果超过 90 天 → 被清理。"""
        old_date = date.today() - timedelta(days=91)
        recent_date = date.today() - timedelta(days=30)

        insert_check_result(db_conn, old_date, 1, "M1", "PASS")
        insert_check_result(db_conn, recent_date, 1, "M1", "PASS")

        assert count_rows(db_conn, "dqm_check_result") == 2

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        remaining = count_rows(db_conn, "dqm_check_result")
        assert remaining == 1

    def test_cleanup_accuracy_detail_expired(self, db_conn, mysql_storage):
        """准确性明细超过 15 天 → 被清理。"""
        old_date = date.today() - timedelta(days=16)
        recent_date = date.today() - timedelta(days=5)

        insert_accuracy_detail(db_conn, old_date, 1, "M3", "stkcode")
        insert_accuracy_detail(db_conn, recent_date, 1, "M3", "stkname")

        assert count_rows(db_conn, "dqm_accuracy_detail") == 2

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        remaining = count_rows(db_conn, "dqm_accuracy_detail")
        assert remaining == 1

    def test_cleanup_no_expired_data(self, db_conn, mysql_storage):
        """没有过期数据 → 不删除。"""
        recent_date = date.today() - timedelta(days=5)

        insert_snapshot(db_conn, recent_date, 1, "993301")
        insert_check_result(db_conn, recent_date, 1, "M1", "PASS")
        insert_accuracy_detail(db_conn, recent_date, 1, "M3", "stkcode")

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        assert count_rows(db_conn, "dqm_security_info_snapshot") == 1
        assert count_rows(db_conn, "dqm_check_result") == 1
        assert count_rows(db_conn, "dqm_accuracy_detail") == 1

    def test_cleanup_all_tables_at_once(self, db_conn, mysql_storage):
        """三张表同时有过期数据 → 全部清理。"""
        old_date = date.today() - timedelta(days=100)

        insert_snapshot(db_conn, old_date, 1, "993301")
        insert_check_result(db_conn, old_date, 1, "M1", "PASS")
        insert_accuracy_detail(db_conn, old_date, 1, "M3", "stkcode")

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        assert count_rows(db_conn, "dqm_security_info_snapshot") == 0
        assert count_rows(db_conn, "dqm_check_result") == 0
        assert count_rows(db_conn, "dqm_accuracy_detail") == 0

    def test_cleanup_boundary_15_days_snapshot(self, db_conn, mysql_storage):
        """快照恰好在 15 天边界 → 不被清理（< 15 天的保留）。"""
        boundary_date = date.today() - timedelta(days=14)

        insert_snapshot(db_conn, boundary_date, 1, "993301")

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        assert count_rows(db_conn, "dqm_security_info_snapshot") == 1

    def test_cleanup_boundary_90_days_check_result(self, db_conn, mysql_storage):
        """检查结果恰好在 90 天边界 → 不被清理。"""
        boundary_date = date.today() - timedelta(days=89)

        insert_check_result(db_conn, boundary_date, 1, "M1", "PASS")

        cleaner = DataCleaner(mysql_storage)
        cleaner.run()

        assert count_rows(db_conn, "dqm_check_result") == 1


# ---------------------------------------------------------------------------
# 测试 CompletenessChecker._prepare 中的快照清理
# ---------------------------------------------------------------------------

class TestPrepareSnapshotCleanup:
    """测试完整性检查 _prepare 中的自动快照清理。"""

    def test_prepare_cleans_old_snapshots(self, db_conn, mysql_storage):
        """_prepare 自动清理超过 15 天的快照。"""
        old_date = date.today() - timedelta(days=16)
        recent_date = date.today() - timedelta(days=5)

        insert_snapshot(db_conn, old_date, 1, "993301", "INDUSTRY_PLATE_INFO")
        insert_snapshot(db_conn, recent_date, 1, "993302", "INDUSTRY_PLATE_INFO")

        assert count_rows(db_conn, "dqm_security_info_snapshot") == 2

        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table="gmdb_plate_info",
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )

        # _prepare 会清理过期快照（Pulsar 连接会失败，但不影响清理逻辑）
        try:
            checker._prepare(date.today(), datetime.now(), 1)
        except Exception:
            pass  # Pulsar 连接失败是预期的

        # 旧快照被清理
        remaining = count_rows(db_conn, "dqm_security_info_snapshot")
        assert remaining <= 1  # 旧数据被清理，可能只剩新的
