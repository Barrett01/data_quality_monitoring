"""M5 及时性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
验证 ads_fin_index_compn_stock_interface_ds 表中 send_date 字段的及时性。

M5 与 M2 的关键差异：
- M2 使用 gmdb_plate_info（有 mst_type 字段），按 mst_type 分组查询
- M5 使用 ads_fin_index_compn_stock_interface_ds（无 mst_type 字段），简单查询 send_date 记录数
- M5 FAIL 告警为 "PLATE_STOCKS 数据未及时到达"

测试场景矩阵：
┌──────────────┬──────────────┬───────────────────────────────────────┐
│ 数据一致性    │ 日期一致性    │ 预期结果                               │
├──────────────┼──────────────┼───────────────────────────────────────┤
│ 有数据       │ 当天          │ PASS — 数据到达，不告警                  │
│ 无数据       │ 当天          │ FAIL — PLATE_STOCKS 数据未及时到达       │
│ 有数据       │ 非当天        │ FAIL — 检查日无数据                      │
│ 有数据(昨天)  │ 今天          │ FAIL — 告警包含 send_date               │
└──────────────┴──────────────┴───────────────────────────────────────┘
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import patch

import pymysql
import pytest
from loguru import logger

from config.constants import CheckResult, MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.timeliness import TimelinessChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"

# 真实线上表（无 mst_type 字段）
ONLINE_TABLE = "ads_fin_index_compn_stock_interface_ds"
# 模拟测试表（模拟线上表结构，可控数据）
MOCK_TABLE = "dqm_test_ads_fin_index"


# ---------------------------------------------------------------------------
# Loguru 告警捕获器
# ---------------------------------------------------------------------------

class AlertCapture:
    """捕获 loguru 输出的 ERROR 级别告警消息。"""

    def __init__(self):
        self.records: list[dict] = []

    def write(self, message):
        record = message.record
        self.records.append({
            "level": record["level"].name,
            "message": record["message"],
        })

    def get_error_alerts(self) -> list[str]:
        return [r["message"] for r in self.records if r["level"] in ("ERROR", "CRITICAL")]

    def has_error(self) -> bool:
        return len(self.get_error_alerts()) > 0

    def clear(self):
        self.records.clear()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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
    """确保 DQM 监控表存在。"""
    cur = db_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    for ddl in ALL_DDL:
        cur.execute(ddl)
    cur.close()


@pytest.fixture(scope="module")
def ensure_mock_table(db_conn):
    """创建模拟 ads_fin_index_compn_stock_interface_ds 的测试表。"""
    cur = db_conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{MOCK_TABLE}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `stkcode` VARCHAR(30) DEFAULT NULL,
            `compn_stock_code` VARCHAR(30) DEFAULT NULL,
            `compn_stock_name` VARCHAR(50) DEFAULT NULL,
            `index_name` VARCHAR(50) DEFAULT NULL,
            `send_date` INT(11) DEFAULT NULL,
            `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX `idx_send_date` (`send_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M5集成测试-模拟ads_fin_index表'
    """)
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{MOCK_TABLE}`")
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


@pytest.fixture
def checker(mysql_storage):
    """创建使用真实 ads_fin_index 表的 M5 checker。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M5,
        table=ONLINE_TABLE,
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def mock_checker(mysql_storage):
    """创建使用 mock 表的 M5 checker。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M5,
        table=MOCK_TABLE,
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def alert_capture():
    capture = AlertCapture()
    sink_id = logger.add(capture.write, level="WARNING")
    yield capture
    logger.remove(sink_id)


@pytest.fixture(autouse=True)
def cleanup(db_conn, ensure_mock_table):
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    try:
        cur.execute(f"DELETE FROM `{MOCK_TABLE}`")
    except pymysql.err.ProgrammingError:
        pass
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    try:
        cur.execute(f"DELETE FROM `{MOCK_TABLE}`")
    except pymysql.err.ProgrammingError:
        pass
    cur.close()


# ---------------------------------------------------------------------------
# 辅助方法
# ---------------------------------------------------------------------------

def get_send_dates(db_conn, table: str = ONLINE_TABLE) -> list[int]:
    cur = db_conn.cursor()
    cur.execute(f"SELECT DISTINCT send_date FROM `{table}` WHERE send_date IS NOT NULL")
    dates = [r[0] for r in cur.fetchall()]
    cur.close()
    return sorted(dates)


def insert_mock_data(db_conn, rows: list[dict]):
    cur = db_conn.cursor()
    for r in rows:
        cur.execute(
            f"INSERT INTO `{MOCK_TABLE}` "
            f"(stkcode, compn_stock_code, compn_stock_name, index_name, send_date) "
            f"VALUES (%s, %s, %s, %s, %s)",
            (r.get("stkcode"), r.get("compn_stock_code"), r.get("compn_stock_name"),
             r.get("index_name"), r.get("send_date")),
        )
    cur.close()


def get_check_result(db_conn, monitor_id: str = "M5", check_date: date = None) -> dict | None:
    cur = db_conn.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM dqm_check_result WHERE monitor_id = %s"
    params = [monitor_id]
    if check_date:
        sql += " AND check_date = %s"
        params.append(check_date)
    sql += " ORDER BY id DESC LIMIT 1"
    cur.execute(sql, tuple(params))
    row = cur.fetchone()
    cur.close()
    return row


def int_to_date(send_date_int: int) -> date:
    return date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)


# ---------------------------------------------------------------------------
# 1. 数据库连接与表验证
# ---------------------------------------------------------------------------

class TestDatabaseSetup:
    def test_ads_fin_table_exists(self, ensure_dqm_tables, db_conn):
        """ads_fin_index_compn_stock_interface_ds 表存在。"""
        cur = db_conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {ONLINE_TABLE}")
        count = cur.fetchone()[0]
        assert count > 0
        cur.close()

    def test_ads_fin_has_send_date(self, ensure_dqm_tables, db_conn):
        """表中应有 send_date 数据。"""
        dates = get_send_dates(db_conn)
        assert len(dates) > 0, "应有 send_date 数据"

    def test_ads_fin_no_mst_type_column(self, ensure_dqm_tables, db_conn):
        """ads_fin_index_compn_stock_interface_ds 表没有 mst_type 字段。"""
        cur = db_conn.cursor()
        cur.execute(f"DESCRIBE {ONLINE_TABLE}")
        columns = [r[0] for r in cur.fetchall()]
        cur.close()
        assert "mst_type" not in columns, "ads_fin 表不应有 mst_type 字段"


# ---------------------------------------------------------------------------
# 2. 场景一：真实表有数据 + 日期一致 → PASS
# ---------------------------------------------------------------------------

class TestScenario1RealTableDataConsistentDateConsistent:
    """场景一：真实表有数据 + 日期一致 → PASS。"""

    def test_check_pass_with_existing_date(self, checker, db_conn):
        """_check：使用真实表中存在的日期 → PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("ads_fin_index_compn_stock_interface_ds 中无 send_date 数据")

        existing_date = int_to_date(dates[0])
        result = checker._check(existing_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] > 0
        # M5 使用 _query_count，details 只有一项
        assert len(result["details"]) == 1
        assert result["details"][0]["mst_type"] == ""
        assert result["details"][0]["count"] > 0

    def test_execute_pass_no_alert(self, checker, db_conn, alert_capture):
        """完整 execute → PASS，确认不产生 ERROR 告警。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("ads_fin_index_compn_stock_interface_ds 中无 send_date 数据")

        existing_date = int_to_date(dates[0])
        checker.execute(check_date=existing_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=existing_date)
        assert row is not None
        assert row["result"] == "PASS"
        assert not alert_capture.has_error(), \
            f"PASS 不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"

        detail = json.loads(row["detail"])
        assert detail["total_count"] > 0


# ---------------------------------------------------------------------------
# 3. 场景二：真实表有数据 + 日期不一致 → FAIL
# ---------------------------------------------------------------------------

class TestScenario2RealTableDataConsistentDateInconsistent:
    """场景二：真实表有数据 + 日期不一致 → FAIL → PLATE_STOCKS 告警。"""

    def test_check_fail_with_wrong_date(self, checker):
        """_check：检查日期不匹配 → FAIL。"""
        wrong_date = date(2099, 12, 31)
        result = checker._check(wrong_date, datetime.now(), 1)

        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0
        assert result["details"] == []
        assert result["send_date"] == 20991231

    def test_execute_fail_with_plate_stocks_alert(self, checker, db_conn, alert_capture):
        """完整 execute → FAIL，确认产生 PLATE_STOCKS 告警。"""
        wrong_date = date(2099, 12, 31)
        checker.execute(check_date=wrong_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=wrong_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 时应该产生 ERROR 告警"

        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text, \
            f"M5 FAIL 告警应包含 PLATE_STOCKS，实际: {alert_text}"

        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0

    def test_execute_fail_alert_prints_table_and_date(self, checker, alert_capture):
        """FAIL 告警中应包含表名和检查日期。"""
        wrong_date = date(2098, 6, 15)
        checker.execute(check_date=wrong_date, check_time=datetime.now(), check_round=1)

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0

        alert_text = " ".join(alerts)
        assert "ads_fin_index_compn_stock_interface_ds" in alert_text, \
            f"告警应包含表名，实际: {alert_text}"
        assert "PLATE_STOCKS" in alert_text, \
            f"M5 告警应包含 PLATE_STOCKS，实际: {alert_text}"


# ---------------------------------------------------------------------------
# 4. 场景三：mock 表为空 → FAIL
# ---------------------------------------------------------------------------

class TestScenario3MockTableEmpty:
    """场景三：mock 表完全为空 → FAIL → PLATE_STOCKS 告警。"""

    def test_check_fail_empty_table(self, mock_checker, db_conn):
        """mock 表为空 → FAIL。"""
        check_date = date(2026, 4, 22)
        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0
        assert result["details"] == []

    def test_execute_fail_with_alert(self, mock_checker, db_conn, alert_capture):
        """完整 execute → FAIL + PLATE_STOCKS 告警。"""
        check_date = date(2026, 4, 22)
        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text


# ---------------------------------------------------------------------------
# 5. 场景四：mock 表有当天数据 → PASS
# ---------------------------------------------------------------------------

class TestScenario4MockTableHasTodayData:
    """场景四：mock 表有当天数据 → PASS。"""

    def test_check_pass_with_today_data(self, mock_checker, db_conn):
        """mock 表有当天数据 → PASS。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": send_date_val},
            {"stkcode": "0001.BK", "compn_stock_code": "SH600001", "compn_stock_name": "邯郸钢铁",
             "index_name": "上证50", "send_date": send_date_val},
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行",
             "index_name": "深证成指", "send_date": send_date_val},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 3
        # M5 使用 _query_count，details 只有一项
        assert len(result["details"]) == 1
        assert result["details"][0]["count"] == 3

    def test_execute_pass_no_alert(self, mock_checker, db_conn, alert_capture):
        """完整 execute → PASS，无 ERROR 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": send_date_val},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "PASS"
        assert not alert_capture.has_error(), \
            f"PASS 不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"

        detail = json.loads(row["detail"])
        assert detail["total_count"] == 1


# ---------------------------------------------------------------------------
# 6. 场景五：mock 表只有昨天数据，检查今天 → FAIL
# ---------------------------------------------------------------------------

class TestScenario5MockTableOnlyYesterdayData:
    """场景五：mock 表只有昨天数据，检查今天 → FAIL + PLATE_STOCKS 告警。"""

    def test_check_fail_yesterday_data_only(self, mock_checker, db_conn):
        """只有昨天数据 → 查今天 → FAIL。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": yesterday},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0

    def test_execute_fail_with_alert(self, mock_checker, db_conn, alert_capture):
        """完整 execute → FAIL + PLATE_STOCKS 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": yesterday},
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行",
             "index_name": "深证成指", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0
        assert detail["send_date"] == 20260422

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text


# ---------------------------------------------------------------------------
# 7. 场景六：mock 表混合日期（部分今天部分昨天）→ PASS
# ---------------------------------------------------------------------------

class TestScenario6MockTableMixedDates:
    """场景六：mock 表混合日期 → 有今天数据 → PASS。"""

    def test_check_pass_mixed_dates(self, mock_checker, db_conn):
        """部分今天部分昨天 → 有今天数据 → PASS。"""
        check_date = date(2026, 4, 22)
        today = 20260422
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": today},
        ])
        insert_mock_data(db_conn, [
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行",
             "index_name": "深证成指", "send_date": yesterday},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        # M5 只查当天数据
        assert result["total_count"] == 1


# ---------------------------------------------------------------------------
# 8. _record 结果真实写入测试
# ---------------------------------------------------------------------------

class TestRecordWithRealDB:
    """测试 M5 检查结果写入 dqm_check_result。"""

    def test_record_inserts_into_db(self, mock_checker, db_conn):
        """_record 应将 M5 结果写入 dqm_check_result。"""
        check_date = date(2026, 4, 22)
        check_time = datetime(2026, 4, 22, 8, 42, 0)
        check_round = 1

        result = {
            "status": CheckResult.PASS,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 7,
            "details": [{"mst_type": "", "count": 7}],
        }

        mock_checker._record(check_date, check_time, check_round, result)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["monitor_id"] == "M5"
        assert row["dimension"] == "及时性"
        assert row["result"] == "PASS"

        detail = json.loads(row["detail"])
        assert detail["send_date"] == 20260422
        assert detail["total_count"] == 7

    def test_record_fail_result(self, mock_checker, db_conn):
        """FAIL 结果写入数据库。"""
        check_date = date(2026, 4, 22)
        check_time = datetime(2026, 4, 22, 8, 42, 0)
        check_round = 1

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
        }

        mock_checker._record(check_date, check_time, check_round, result)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

    def test_record_upsert_idempotent(self, mock_checker, db_conn):
        """upsert 幂等性。"""
        check_date = date(2026, 4, 22)
        check_time = datetime(2026, 4, 22, 8, 42, 0)
        check_round = 1

        mock_checker._record(check_date, check_time, check_round, {
            "status": CheckResult.PASS,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 5,
            "details": [{"mst_type": "", "count": 5}],
        })

        mock_checker._record(check_date, check_time, check_round, {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
        })

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND monitor_id = %s",
            (check_date, "M5"),
        )
        rows = cur.fetchall()
        cur.close()

        assert len(rows) == 1
        assert rows[0]["result"] == "FAIL"


# ---------------------------------------------------------------------------
# 9. 端到端 execute 测试
# ---------------------------------------------------------------------------

class TestExecuteEndToEnd:
    """端到端测试：完整 execute 流程。"""

    def test_e2e_pass(self, mock_checker, db_conn):
        """端到端：有当天数据 → PASS。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": send_date_val},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "PASS"

    def test_e2e_fail(self, mock_checker, db_conn):
        """端到端：无当天数据 → FAIL。"""
        check_date = date(2026, 4, 22)
        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        detail = json.loads(row["detail"])
        assert detail["send_date"] == 20260422
        assert detail["total_count"] == 0

    def test_e2e_fail_with_yesterday_data(self, mock_checker, db_conn):
        """端到端：只有昨天数据 → FAIL。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"


# ---------------------------------------------------------------------------
# 10. 逐场景完整 execute 串联测试
# ---------------------------------------------------------------------------

class TestScenarioByScenarioExecute:
    """逐个场景完整 execute 串联测试。"""

    def test_s1_real_table_pass(self, checker, db_conn, alert_capture):
        """S1: 真实表有数据 + 日期一致 → PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("ads_fin_index_compn_stock_interface_ds 中无 send_date 数据")

        check_date = int_to_date(dates[0])
        checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "PASS"
        assert not alert_capture.has_error()

    def test_s2_real_table_fail(self, checker, db_conn, alert_capture):
        """S2: 真实表 + 日期不一致 → FAIL + PLATE_STOCKS 告警。"""
        check_date = date(2099, 12, 31)
        checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text

    def test_s3_mock_empty_fail(self, mock_checker, db_conn, alert_capture):
        """S3: mock 表为空 → FAIL + PLATE_STOCKS 告警。"""
        check_date = date(2026, 4, 22)
        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text

    def test_s4_mock_today_pass(self, mock_checker, db_conn, alert_capture):
        """S4: mock 表有当天数据 → PASS。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": send_date_val},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "PASS"
        assert not alert_capture.has_error()

    def test_s5_mock_yesterday_fail(self, mock_checker, db_conn, alert_capture):
        """S5: mock 表只有昨天数据 → FAIL + PLATE_STOCKS 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
             "index_name": "上证50", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "PLATE_STOCKS 数据未及时到达" in alert_text
