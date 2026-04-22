"""M3 准确性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
验证 gmdb_plate_info 表中字段非空和类型约束的准确性检查。

测试场景矩阵：
┌──────────────────────────────────────────┬───────────────────────────────────────┐
│ 场景                                      │ 预期结果                               │
├──────────────────────────────────────────┼───────────────────────────────────────┤
│ S1: 数据全部合法                          │ PASS — 不告警                          │
│ S2: 存在 NULL 必填字段                    │ FAIL — 告警 NULL_VALUE                 │
│ S3: 存在正则不匹配字段                    │ FAIL — 告警 REGEX_MISMATCH             │
│ S4: 存在枚举值非法                        │ FAIL — 告警 ENUM_INVALID               │
│ S5: 多个必填字段同时为空                  │ FAIL — 告警多条 NULL_VALUE            │
│ S6: 多种错误混合                          │ FAIL — 告警包含多种错误类型              │
│ S7: 非必填字段为空                        │ PASS — 不报错                          │
│ S8: 无当天数据                            │ PASS（0 条校验，无错误）                 │
└──────────────────────────────────────────┴───────────────────────────────────────┘

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
from src.dqm.checkers.accuracy import AccuracyChecker
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
    cur = db_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    for ddl in ALL_DDL:
        cur.execute(ddl)
    cur.close()


@pytest.fixture(scope="module")
def ensure_mock_table(db_conn):
    cur = db_conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{MOCK_TABLE}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `stkcode` VARCHAR(20) DEFAULT NULL,
            `stkname` VARCHAR(100) DEFAULT NULL,
            `std_stkcode` VARCHAR(30) DEFAULT NULL,
            `zhishubankuaileibie` VARCHAR(100) DEFAULT NULL,
            `mst_type` VARCHAR(50) DEFAULT NULL,
            `send_date` INT(11) DEFAULT NULL,
            `send_time` INT(11) DEFAULT NULL,
            `index_type_name` VARCHAR(30) DEFAULT NULL,
            `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX `idx_send_date` (`send_date`),
            INDEX `idx_mst_type` (`mst_type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M3集成测试-模拟plate_info表'
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
    """创建 M3 AccuracyChecker 实例（查真实表）。"""
    return AccuracyChecker(
        monitor_id=MonitorID.M3,
        table=TEST_TABLE,
        key_field="stkcode",
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def mock_checker(mysql_storage):
    """创建使用 mock 表的 M3 AccuracyChecker 实例。"""
    return AccuracyChecker(
        monitor_id=MonitorID.M3,
        table=MOCK_TABLE,
        key_field="stkcode",
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
    cur.execute("DELETE FROM dqm_accuracy_detail")
    try:
        cur.execute(f"DELETE FROM `{MOCK_TABLE}`")
    except pymysql.err.ProgrammingError:
        pass
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_accuracy_detail")
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
            f"INSERT INTO `{MOCK_TABLE}` (stkcode, stkname, std_stkcode, zhishubankuaileibie, mst_type, send_date, send_time, index_type_name) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                r.get("stkcode"),
                r.get("stkname"),
                r.get("std_stkcode"),
                r.get("zhishubankuaileibie"),
                r.get("mst_type"),
                r.get("send_date"),
                r.get("send_time"),
                r.get("index_type_name"),
            ),
        )
    cur.close()


def get_check_result(db_conn, monitor_id: str = "M3", check_date: date = None) -> dict | None:
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


def get_accuracy_details(db_conn, check_date: date, monitor_id: str = "M3") -> list[dict]:
    cur = db_conn.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM dqm_accuracy_detail WHERE monitor_id = %s AND check_date = %s"
    cur.execute(sql, (monitor_id, check_date))
    rows = cur.fetchall()
    cur.close()
    return rows


def int_to_date(send_date_int: int) -> date:
    return date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)


# ---------------------------------------------------------------------------
# 场景一：数据全部合法 → PASS
# ---------------------------------------------------------------------------

class TestScenario1AllValidPass:
    """场景一：所有数据字段合法 → PASS → 不告警。"""

    def test_check_pass_with_real_data(self, checker, db_conn):
        """使用真实线上表已有的日期数据 → _check PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        check_date = int_to_date(dates[0])
        result = checker._check(check_date, datetime.now(), 1)

        # 只要线上表有数据且字段都合法，就应 PASS
        if result["total"] > 0 and result["errors"] == 0:
            assert result["status"] == CheckResult.PASS
        # 如果有错误字段，结果是 FAIL，这也是合法的（取决于真实数据质量）

    def test_execute_pass_with_mock_valid_data(self, mock_checker, db_conn, alert_capture):
        """mock 表中插入合法数据 → execute PASS → 不告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": 90000, "index_type_name": "行业"},
            {"stkcode": "000002", "stkname": "万科A", "std_stkcode": "000002.SZ",
             "zhishubankuaileibie": "区域板块", "mst_type": "REGION_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "PASS"

        # PASS 不应有 ERROR 告警
        assert not alert_capture.has_error(), \
            f"PASS 不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"

        # 无准确性明细
        details = get_accuracy_details(db_conn, check_date)
        assert len(details) == 0

        print(f">>> S1 PASS | check_date={check_date}, 合法数据, 无告警")


# ---------------------------------------------------------------------------
# 场景二：存在 NULL 必填字段 → FAIL
# ---------------------------------------------------------------------------

class TestScenario2NullValueFail:
    """场景二：必填字段为 NULL → FAIL → 告警 NULL_VALUE。"""

    def test_null_stkname_fail(self, mock_checker, db_conn, alert_capture):
        """stkname 为 NULL → FAIL + NULL_VALUE 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None
        assert row["result"] == "FAIL"

        # 验证告警
        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0, "FAIL 必须产生 ERROR 告警"
        alert_text = " ".join(alerts)
        assert "数据异常" in alert_text

        # 验证明细表有 NULL_VALUE 错误
        details = get_accuracy_details(db_conn, check_date)
        null_errors = [d for d in details if d["error_type"] == "NULL_VALUE"]
        assert len(null_errors) >= 1

        # 验证 detail JSON
        detail_json = json.loads(row["detail"])
        assert detail_json["errors"] >= 1

        print(f">>> S2 FAIL | NULL_VALUE 告警: {alerts}, 明细: {[d['field_name'] for d in null_errors]}")

    def test_empty_string_stkcode_fail(self, mock_checker, db_conn, alert_capture):
        """stkcode 为空字符串 → FAIL + NULL_VALUE 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        null_errors = [d for d in details if d["error_type"] == "NULL_VALUE" and d["field_name"] == "stkcode"]
        assert len(null_errors) >= 1

        print(f">>> S2 FAIL | 空字符串 stkcode → NULL_VALUE")


# ---------------------------------------------------------------------------
# 场景三：正则不匹配 → FAIL
# ---------------------------------------------------------------------------

class TestScenario3RegexMismatchFail:
    """场景三：字段值不匹配正则 → FAIL → 告警 REGEX_MISMATCH。"""

    def test_stkcode_regex_fail(self, mock_checker, db_conn, alert_capture):
        """stkcode 不匹配 ^[0-9]{6}$ → FAIL + REGEX_MISMATCH。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "123", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        regex_errors = [d for d in details if d["error_type"] == "REGEX_MISMATCH" and d["field_name"] == "stkcode"]
        assert len(regex_errors) >= 1

        print(f">>> S3 FAIL | REGEX_MISMATCH: {[d['error_value'] for d in regex_errors]}")

    def test_std_stkcode_regex_fail(self, mock_checker, db_conn, alert_capture):
        """std_stkcode 不匹配 ^[0-9]{6}\\.[A-Z]{2}$ → FAIL + REGEX_MISMATCH。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "测试", "std_stkcode": "bad_format",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        regex_errors = [d for d in details if d["error_type"] == "REGEX_MISMATCH" and d["field_name"] == "std_stkcode"]
        assert len(regex_errors) >= 1

        print(f">>> S3 FAIL | std_stkcode REGEX_MISMATCH")


# ---------------------------------------------------------------------------
# 场景四：枚举值非法 → FAIL
# ---------------------------------------------------------------------------

class TestScenario4EnumInvalidFail:
    """场景四：mst_type 枚举值非法 → FAIL → 告警 ENUM_INVALID。"""

    def test_invalid_mst_type(self, mock_checker, db_conn, alert_capture):
        """mst_type 不在枚举集合中 → FAIL + ENUM_INVALID。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INVALID_TYPE",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        enum_errors = [d for d in details if d["error_type"] == "ENUM_INVALID" and d["field_name"] == "mst_type"]
        assert len(enum_errors) >= 1
        assert enum_errors[0]["error_value"] == "INVALID_TYPE"

        print(f">>> S4 FAIL | ENUM_INVALID: {enum_errors[0]['error_value']}")


# ---------------------------------------------------------------------------
# 场景五：数值类型错误 → FAIL
# ---------------------------------------------------------------------------

class TestScenario5MultipleNullsFail:
    """场景五：多个必填字段同时为空 → FAIL → 告警多条 NULL_VALUE。"""

    def test_multiple_null_fields(self, mock_checker, db_conn, alert_capture):
        """stkname + zhishubankuaileibie 同时为 NULL → FAIL + 多条 NULL_VALUE。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": None, "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        null_errors = [d for d in details if d["error_type"] == "NULL_VALUE"]
        # 至少 stkname + zhishubankuaileibie 两个 NULL
        assert len(null_errors) >= 2
        null_fields = {d["field_name"] for d in null_errors}
        assert "stkname" in null_fields
        assert "zhishubankuaileibie" in null_fields

        print(f">>> S5 FAIL | 多个 NULL_VALUE: {null_fields}")


# ---------------------------------------------------------------------------
# 场景六：多种错误混合 → FAIL
# ---------------------------------------------------------------------------

class TestScenario6MixedErrorsFail:
    """场景六：多种错误同时存在 → FAIL → 告警包含多种错误类型。"""

    def test_mixed_null_regex_enum_errors(self, mock_checker, db_conn, alert_capture):
        """一行数据中同时存在 NULL + REGEX + ENUM 错误。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "bad",
             "zhishubankuaileibie": "行业板块", "mst_type": "INVALID",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        error_types = {d["error_type"] for d in details}
        # 至少应有 NULL_VALUE (stkname), REGEX_MISMATCH (std_stkcode), ENUM_INVALID (mst_type)
        assert "NULL_VALUE" in error_types
        assert "REGEX_MISMATCH" in error_types
        assert "ENUM_INVALID" in error_types

        alerts = alert_capture.get_error_alerts()
        assert len(alerts) > 0
        alert_text = " ".join(alerts)
        assert "数据异常" in alert_text

        print(f">>> S6 FAIL | 混合错误类型: {error_types}")

    def test_multiple_rows_with_different_errors(self, mock_checker, db_conn, alert_capture):
        """多行数据，不同行有不同错误 → FAIL + 多条明细。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
            {"stkcode": "000002", "stkname": "万科A", "std_stkcode": "000002.SZ",
             "zhishubankuaileibie": "区域板块", "mst_type": "BAD_TYPE",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"

        details = get_accuracy_details(db_conn, check_date)
        assert len(details) >= 2  # 至少 2 条错误

        print(f">>> S6 FAIL | 多行错误, 明细数: {len(details)}")


# ---------------------------------------------------------------------------
# 场景七：非必填字段为空 → PASS
# ---------------------------------------------------------------------------

class TestScenario7OptionalNullPass:
    """场景七：非必填字段为空 → PASS → 不报错。"""

    def test_optional_fields_null_pass(self, mock_checker, db_conn, alert_capture):
        """send_time 和 index_type_name 为 NULL（非必填）→ PASS。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "PASS"

        # 非必填字段为空不应产生告警
        assert not alert_capture.has_error(), \
            f"非必填字段为空不应产生 ERROR 告警，但发现: {alert_capture.get_error_alerts()}"

        details = get_accuracy_details(db_conn, check_date)
        assert len(details) == 0

        print(f">>> S7 PASS | 非必填字段为空, 无告警")


# ---------------------------------------------------------------------------
# 场景八：无当天数据 → PASS（0 条校验）
# ---------------------------------------------------------------------------

class TestScenario8NoDataPass:
    """场景八：当天无数据 → 0 条校验 → PASS。"""

    def test_no_data_on_check_date(self, mock_checker, db_conn, alert_capture):
        """mock 表为空 → 0 条校验 → PASS。"""
        check_date = date(2026, 4, 22)

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row is not None

        # 0 条数据，0 个错误 → PASS
        detail = json.loads(row["detail"])
        assert detail["total"] == 0
        assert detail["errors"] == 0

        print(f">>> S8 PASS | 无当天数据, total=0, errors=0")


# ---------------------------------------------------------------------------
# 逐场景完整 execute 串联测试
# ---------------------------------------------------------------------------

class TestScenarioByScenarioExecute:
    """逐个场景完整 execute 串联测试。

    方便单独运行某一个场景：
        pytest test_m3_accuracy.py::TestScenarioByScenarioExecute::test_s1 -v
        pytest test_m3_accuracy.py::TestScenarioByScenarioExecute::test_s2 -v
        ...
    """

    def test_s1_all_valid_pass(self, mock_checker, db_conn, alert_capture):
        """S1: 全部合法 → PASS → 不告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": 90000, "index_type_name": "行业"},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "PASS"
        assert not alert_capture.has_error()

        print(f">>> S1 PASS | 全部合法, 无告警")

    def test_s2_null_value_fail(self, mock_checker, db_conn, alert_capture):
        """S2: 必填字段 NULL → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"
        assert alert_capture.has_error()

        details = get_accuracy_details(db_conn, check_date)
        null_errors = [d for d in details if d["error_type"] == "NULL_VALUE"]
        assert len(null_errors) >= 1

        print(f">>> S2 FAIL | NULL_VALUE 告警")

    def test_s3_regex_mismatch_fail(self, mock_checker, db_conn, alert_capture):
        """S3: 正则不匹配 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "123", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"
        assert alert_capture.has_error()

        details = get_accuracy_details(db_conn, check_date)
        regex_errors = [d for d in details if d["error_type"] == "REGEX_MISMATCH"]
        assert len(regex_errors) >= 1

        print(f">>> S3 FAIL | REGEX_MISMATCH 告警")

    def test_s4_enum_invalid_fail(self, mock_checker, db_conn, alert_capture):
        """S4: 枚举值非法 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "BAD_TYPE",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"
        assert alert_capture.has_error()

        details = get_accuracy_details(db_conn, check_date)
        enum_errors = [d for d in details if d["error_type"] == "ENUM_INVALID"]
        assert len(enum_errors) >= 1

        print(f">>> S4 FAIL | ENUM_INVALID 告警")

    def test_s5_multiple_nulls_fail(self, mock_checker, db_conn, alert_capture):
        """S5: 多个必填字段为空 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": None, "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"
        assert alert_capture.has_error()

        details = get_accuracy_details(db_conn, check_date)
        null_errors = [d for d in details if d["error_type"] == "NULL_VALUE"]
        assert len(null_errors) >= 2

        print(f">>> S5 FAIL | 多个 NULL_VALUE 告警")

    def test_s6_mixed_errors_fail(self, mock_checker, db_conn, alert_capture):
        """S6: 多种错误混合 → FAIL → 告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "123", "stkname": None, "std_stkcode": "bad",
             "zhishubankuaileibie": "行业板块", "mst_type": "INVALID",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "FAIL"
        assert alert_capture.has_error()

        details = get_accuracy_details(db_conn, check_date)
        error_types = {d["error_type"] for d in details}
        assert len(error_types) >= 3  # 至少 3 种错误类型

        print(f">>> S6 FAIL | 混合错误, 类型: {error_types}")

    def test_s7_optional_null_pass(self, mock_checker, db_conn, alert_capture):
        """S7: 非必填字段为空 → PASS → 不告警。"""
        check_date = date(2026, 4, 22)
        send_date_val = 20260422

        insert_mock_data(db_conn, [
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业板块", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": send_date_val, "send_time": None, "index_type_name": None},
        ])

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        assert row["result"] == "PASS"
        assert not alert_capture.has_error()

        print(f">>> S7 PASS | 非必填字段为空, 无告警")

    def test_s8_no_data_pass(self, mock_checker, db_conn, alert_capture):
        """S8: 无当天数据 → 0 条校验 → PASS。"""
        check_date = date(2026, 4, 22)

        mock_checker.execute(check_date=check_date, check_time=datetime.now(), check_round=1)

        row = get_check_result(db_conn, check_date=check_date)
        detail = json.loads(row["detail"])
        assert detail["total"] == 0
        assert detail["errors"] == 0

        print(f">>> S8 PASS | 无当天数据, total=0")
