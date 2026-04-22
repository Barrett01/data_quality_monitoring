"""M2 及时性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
验证 gmdb_plate_info 表中 send_date 字段的及时性。

测试场景矩阵：
┌──────────────┬──────────────┬───────────────────────────────────────┐
│ 数据一致性    │ 日期一致性    │ 预期结果                               │
├──────────────┼──────────────┼───────────────────────────────────────┤
│ 一致（有数据）│ 一致（当天）  │ PASS — 数据到达，不告警                  │
│ 一致（有数据）│ 不一致（非当天）│ FAIL — 检查日无数据，告警                │
│ 不一致（部分）│ 一致（当天）  │ FAIL — 部分类型缺失，告警指出缺什么       │
│ 不一致（部分）│ 不一致（非当天）│ FAIL — 既无当天数据又类型不全，告警      │
└──────────────┴──────────────┴───────────────────────────────────────┘

注意：项目使用 loguru（非标准 logging），测试通过 loguru sink 捕获告警输出。
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

TEST_TABLE = "gmdb_plate_info"
MOCK_TABLE = "dqm_test_gmdb_plate_info"


# ---------------------------------------------------------------------------
# Loguru 告警捕获器
# ---------------------------------------------------------------------------

class AlertCapture:
    """捕获 loguru 输出的 ERROR 级别告警消息。"""

    def __init__(self):
        self.records: list[dict] = []

    def write(self, message):
        """loguru sink 回调：每条日志都会调用。"""
        record = message.record
        self.records.append({
            "level": record["level"].name,
            "message": record["message"],
        })

    def get_error_alerts(self) -> list[str]:
        """获取所有 ERROR 及以上级别的告警消息。"""
        return [r["message"] for r in self.records if r["level"] in ("ERROR", "CRITICAL")]

    def has_error(self) -> bool:
        """是否存在 ERROR 告警。"""
        return len(self.get_error_alerts()) > 0

    def clear(self):
        self.records.clear()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def init_logger():
    """模块级别：初始化日志。"""
    setup_logger()


@pytest.fixture(scope="module")
def db_conn():
    """模块级别：原始数据库连接。"""
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
    """模块级别：确保 DQM 监控表存在。"""
    cur = db_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    for ddl in ALL_DDL:
        cur.execute(ddl)
    cur.close()
    return True


@pytest.fixture(scope="module")
def ensure_mock_table(db_conn):
    """模块级别：创建 mock 表。"""
    cur = db_conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{MOCK_TABLE}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `stkcode` VARCHAR(20) DEFAULT NULL,
            `stkname` VARCHAR(100) DEFAULT NULL,
            `mst_type` VARCHAR(50) DEFAULT NULL,
            `send_date` INT(11) DEFAULT NULL,
            `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX `idx_send_date` (`send_date`),
            INDEX `idx_mst_type` (`mst_type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M2集成测试-模拟plate_info表'
    """)
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{MOCK_TABLE}`")
    cur.close()


@pytest.fixture
def mysql_storage():
    """每个测试用例：创建 MySQLStorage 实例。"""
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
    """创建 M2 TimelinessChecker 实例（查真实表）。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M2,
        table=TEST_TABLE,
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def mock_checker(mysql_storage):
    """创建使用 mock 表的 M2 TimelinessChecker 实例。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M2,
        table=MOCK_TABLE,
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def alert_capture():
    """每个测试：注册 loguru sink 捕获告警，测试结束后移除。"""
    capture = AlertCapture()
    sink_id = logger.add(capture.write, level="WARNING")
    yield capture
    logger.remove(sink_id)


@pytest.fixture(autouse=True)
def cleanup(db_conn, ensure_mock_table):
    """每个测试前后清理数据。"""
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

def get_send_dates(db_conn, table: str = TEST_TABLE) -> list[int]:
    cur = db_conn.cursor()
    cur.execute(f"SELECT DISTINCT send_date FROM `{table}` WHERE send_date IS NOT NULL")
    dates = [r[0] for r in cur.fetchall()]
    cur.close()
    return sorted(dates)


def insert_mock_data(db_conn, rows: list[dict]):
    cur = db_conn.cursor()
    for r in rows:
        cur.execute(
            f"INSERT INTO `{MOCK_TABLE}` (stkcode, stkname, mst_type, send_date) VALUES (%s, %s, %s, %s)",
            (r.get("stkcode"), r.get("stkname"), r.get("mst_type"), r.get("send_date")),
        )
    cur.close()


def get_check_result(db_conn, monitor_id: str = "M2", check_date: date = None) -> dict | None:
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
# 场景一：数据一致 + 日期一致 → PASS
# ---------------------------------------------------------------------------

class TestScenario1DataConsistentDateConsistent:
    """场景一：数据一致 + 日期一致 → PASS → 不告警。"""

    def test_check_pass_with_existing_date(self, checker, db_conn):
        """_check：使用数据库中实际存在的日期 → PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        existing_date = int_to_date(dates[0])
        result = checker._check(existing_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] > 0
        assert len(result["details"]) > 0
        for d in result["details"]:
            assert "mst_type" in d
            assert "count" in d
            assert d["count"] > 0

    def test_execute_pass_no_alert(self, checker, db_conn, alert_capture):
        """完整 execute → PASS，确认不产生 ERROR 告警。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        existing_date = int_to_date(dates[0])
        checker.execute(check_date=existing_date, check_time=datetime.now(), check_round=1)

        # 验证数据库结果为 PASS
        row = get_check_result(db_conn, check_date=existing_date)
        assert row is not None
        assert row["result"] == "PASS"

        # PASS 时不应该有 ERROR 告警
        assert not alert_capture.has_error(), \
            f"PASS 不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"

        detail = json.loads(row["detail"])
        assert detail["total_count"] > 0

        print(f">>> 场景1 PASS | check_date={existing_date}, 无告警, total_count={detail['total_count']}")


# ---------------------------------------------------------------------------
# 场景二：数据一致 + 日期不一致 → FAIL
# ---------------------------------------------------------------------------

class TestScenario2DataConsistentDateInconsistent:
    """场景二：数据一致 + 日期不一致 → FAIL → 告警打印 send_date。"""

    def test_check_fail_with_wrong_date(self, checker):
        """_check：检查日期不匹配 → FAIL。"""
        wrong_date = date(2099, 12, 31)
        result = checker._check(wrong_date, datetime.now(), 1)

        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0
        assert result["details"] == []
        assert result["send_date"] == 20991231

    def test_execute_fail_with_alert(self, checker, db_conn, alert_capture):
        """完整 execute → FAIL，确认产生 ERROR 告警并打印告警信息。"""
        wrong_date = date(2099, 12, 31)
        checker.execute(check_date=wrong_date, check_time=datetime.now(), check_round=1)

        # 验证数据库结果为 FAIL
        row = get_check_result(db_conn, check_date=wrong_date)
        assert row is not None
        assert row["result"] == "FAIL"

        # FAIL 时必须有 ERROR 告警
        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 时应该产生 ERROR 告警，但未发现"

        # 告警内容应包含 send_date 或 "无当天数据"
        alert_text = " ".join(alerts)
        assert "无当天数据" in alert_text or "send_date" in alert_text, \
            f"告警信息应包含 send_date，实际: {alert_text}"

        # 验证 detail
        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0

        print(f">>> 场景2 FAIL | check_date={wrong_date}, 告警: {alerts}")

    def test_execute_fail_alert_prints_table_and_date(self, checker, alert_capture):
        """FAIL 告警中应包含表名和检查日期。"""
        wrong_date = date(2098, 6, 15)
        checker.execute(check_date=wrong_date, check_time=datetime.now(), check_round=1)

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0

        alert_text = " ".join(alerts)
        assert "gmdb_plate_info" in alert_text, f"告警应包含表名，实际: {alert_text}"
        assert "无当天数据" in alert_text or "20980615" in alert_text, \
            f"告警应包含日期信息，实际: {alert_text}"

        print(f">>> 场景2 告警含表名和日期: {alerts}")


# ---------------------------------------------------------------------------
# 场景三：数据不一致 + 日期一致 → FAIL
# ---------------------------------------------------------------------------

class TestScenario3DataInconsistentDateConsistent:
    """场景三：数据不一致（部分 mst_type 缺失）+ 日期一致

    A) 完全没有当天数据 → FAIL + 告警
    B) 只有部分类型有当天数据 → PASS，details 反映类型缺失
    C) 大部分类型有但 PLATE_STOCKS 缺失 → PASS，details 显示分布
    """

    def test_no_data_at_all_on_check_date(self, mock_checker, db_conn, alert_capture):
        """3A: mock 表为空 → FAIL + 告警。"""
        check_date = date(2026, 4, 22)
        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 时应该产生 ERROR 告警"
        alert_text = " ".join(alerts)
        assert "无当天数据" in alert_text or "send_date" in alert_text, \
            f"告警信息: {alert_text}"

        print(f">>> 场景3A FAIL 告警: {alerts}")

    def test_partial_mst_types_on_check_date(self, mock_checker, db_conn):
        """3B: 只有部分 mst_type 有数据 → PASS。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": send_date_val},
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": send_date_val},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 2
        assert len(result["details"]) == 1
        assert result["details"][0]["mst_type"] == "INDUSTRY_PLATE_INFO"

    def test_only_plate_stocks_missing_on_check_date(self, mock_checker, db_conn, alert_capture):
        """3C: 大部分类型有数据但 PLATE_STOCKS 缺失 → PASS，details 显示分布，无告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": send_date_val},
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "REGION_PLATE_INFO", "send_date": send_date_val},
            {"stkcode": "000003", "stkname": "国农科技", "mst_type": "HOTIDEA_PLATE_INFO", "send_date": send_date_val},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 3
        mst_types_in_result = {d["mst_type"] for d in result["details"]}
        assert "INDUSTRY_PLATE_INFO" in mst_types_in_result
        assert "REGION_PLATE_INFO" in mst_types_in_result
        assert "HOTIDEA_PLATE_INFO" in mst_types_in_result
        assert "PLATE_STOCKS" not in mst_types_in_result

        # PASS 不应有 ERROR 告警
        mock_checker._alert(check_date, datetime.now(), 1, result)
        assert not alert_capture.has_error(), \
            f"PASS 不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"


# ---------------------------------------------------------------------------
# 场景四：数据不一致 + 日期不一致 → FAIL
# ---------------------------------------------------------------------------

class TestScenario4DataInconsistentDateInconsistent:
    """场景四：数据不一致 + 日期不一致 → FAIL → 告警。"""

    def test_data_on_wrong_date_and_partial_types(self, mock_checker, db_conn, alert_capture):
        """4A: 数据在昨天，检查今天，类型不全 → FAIL + 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": send_date_val},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 必须产生 ERROR 告警"
        alert_text = " ".join(alerts)
        assert "无当天数据" in alert_text or "send_date" in alert_text, \
            f"告警信息: {alert_text}"

        print(f">>> 场景4A FAIL 告警: {alerts}")

    def test_all_data_on_yesterday_check_today(self, mock_checker, db_conn, alert_capture):
        """4B: 昨天数据完整，今天完全空 → FAIL + 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "REGION_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000003", "stkname": "国农科技", "mst_type": "HOTIDEA_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000004", "stkname": "国信证券", "mst_type": "PLATE_STOCKS", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0
        assert detail["send_date"] == 20260422

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 必须产生 ERROR 告警"

        print(f">>> 场景4B FAIL 告警: {alerts}")

    def test_mixed_dates_some_types_today_some_yesterday(self, mock_checker, db_conn):
        """4C: 部分类型有今天数据，部分只有昨天 → PASS。"""
        check_date = date(2026, 4, 22)
        today = 20260422
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": today},
        ])
        insert_mock_data(db_conn, [
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "REGION_PLATE_INFO", "send_date": yesterday},
        ])

        result = mock_checker._check(check_date, datetime.now(), 1)

        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 1
        assert len(result["details"]) == 1
        assert result["details"][0]["mst_type"] == "INDUSTRY_PLATE_INFO"

    def test_only_yesterday_data_no_today_data_at_all(self, mock_checker, db_conn, alert_capture):
        """4D: 只有昨天部分类型，今天完全没数据 → FAIL + 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "无当天数据" in alert_text or "send_date" in alert_text

        print(f">>> 场景4D FAIL 告警: {alerts}")


# ---------------------------------------------------------------------------
# 逐场景完整 execute 串联测试（单独列出，便于一个一个跑）
# ---------------------------------------------------------------------------

class TestScenarioByScenarioExecute:
    """逐个场景完整 execute 串联测试。

    方便单独运行某一个场景：
        pytest test_m2_timeliness.py::TestScenarioByScenarioExecute::test_s1 -v
        pytest test_m2_timeliness.py::TestScenarioByScenarioExecute::test_s2 -v
        ...
    """

    def test_s1_data_consistent_date_consistent_pass(self, checker, db_conn, alert_capture):
        """S1: 数据一致 + 日期一致 → PASS → 不告警。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        check_date = int_to_date(dates[0])
        checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "PASS"

        assert not alert_capture.has_error(), \
            f"S1 PASS 不应告警，但发现: {alert_capture.get_error_alerts()}"

        print(f">>> S1 PASS | check_date={check_date}, 无告警")

    def test_s2_data_consistent_date_inconsistent_fail(self, checker, db_conn, alert_capture):
        """S2: 数据一致 + 日期不一致 → FAIL → 告警打印 send_date。"""
        check_date = date(2099, 12, 31)
        checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "S2 FAIL 必须产生 ERROR 告警"

        alert_text = " ".join(alerts)
        print(f">>> S2 FAIL | check_date={check_date}, 告警: {alerts}")
        assert "无当天数据" in alert_text or "send_date" in alert_text

    def test_s3_data_inconsistent_date_consistent_fail(self, mock_checker, db_conn, alert_capture):
        """S3: 数据不一致（mock 表为空）+ 日期一致 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "S3 FAIL 必须产生 ERROR 告警"

        alert_text = " ".join(alerts)
        print(f">>> S3 FAIL | check_date={check_date}, 告警: {alerts}")
        assert "无当天数据" in alert_text or "send_date" in alert_text

    def test_s3b_data_partial_date_consistent_pass(self, mock_checker, db_conn, alert_capture):
        """S3b: 数据不一致（只有部分类型）+ 日期一致 → PASS → 不告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": send_date_val},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "PASS"

        assert not alert_capture.has_error(), \
            f"S3b PASS 不应告警，但发现: {alert_capture.get_error_alerts()}"

        detail = json.loads(row["detail"])
        mst_types = [d["mst_type"] for d in detail["details"]]
        print(f">>> S3b PASS | check_date={check_date}, 有数据的类型: {mst_types}")
        assert "INDUSTRY_PLATE_INFO" in mst_types

    def test_s4_data_inconsistent_date_inconsistent_fail(self, mock_checker, db_conn, alert_capture):
        """S4: 数据不一致（只有昨天的部分类型）+ 日期不一致 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "REGION_PLATE_INFO", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "S4 FAIL 必须产生 ERROR 告警"

        alert_text = " ".join(alerts)
        print(f">>> S4 FAIL | check_date={check_date}, 告警: {alerts}")
        assert "无当天数据" in alert_text or "send_date" in alert_text

    def test_s4b_all_yesterday_full_types_today_empty_fail(self, mock_checker, db_conn, alert_capture):
        """S4b: 昨天数据完整（4 种类型），今天完全空 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        yesterday = 20260421

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "mst_type": "INDUSTRY_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000002", "stkname": "万科A", "mst_type": "REGION_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000003", "stkname": "国农科技", "mst_type": "HOTIDEA_PLATE_INFO", "send_date": yesterday},
            {"stkcode": "000004", "stkname": "国信证券", "mst_type": "PLATE_STOCKS", "send_date": yesterday},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "S4b FAIL 必须产生 ERROR 告警"

        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0
        assert detail["send_date"] == 20260422

        print(f">>> S4b FAIL | check_date={check_date}, send_date=20260422, total_count=0, 告警: {alerts}")
