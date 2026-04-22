"""M2 及时性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
验证 gmdb_plate_info 表中 send_date 字段的及时性。

测试覆盖：
1. 数据库连接与表初始化
2. _query_by_mst_type — 真实查询 gmdb_plate_info
3. _check — 及时性比对逻辑（使用真实数据）
4. _record — 结果真实写入 dqm_check_result
5. execute 端到端 — _check → _record → _alert
6. 不同数据场景（有当天数据/无当天数据）
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import patch

import pymysql
import pytest

from config.constants import CheckResult, Dimension, MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.timeliness import TimelinessChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import CheckResultRepository
from src.dqm.storage.schema import ALL_DDL

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def init_logger():
    """模块级别：初始化日志。"""
    setup_logger()


@pytest.fixture(scope="module")
def db_conn():
    """模块级别：原始数据库连接，用于表初始化和数据准备。"""
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
def ensure_tables(db_conn):
    """模块级别：确保 DQM 监控表存在（使用代码中的 DDL）。"""
    cur = db_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    for ddl in ALL_DDL:
        cur.execute(ddl)
    cur.close()
    return True


@pytest.fixture
def mysql_storage():
    """每个测试用例：创建 MySQLStorage 实例，测试后关闭。"""
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
    """创建 M2 TimelinessChecker 实例。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M2,
        table="gmdb_plate_info",
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 21), datetime(2026, 4, 21, 9, 0, 0), 1


@pytest.fixture(autouse=True)
def cleanup_check_result(db_conn):
    """每个测试前后清理 dqm_check_result 中的测试数据。"""
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.close()


# ---------------------------------------------------------------------------
# 辅助方法
# ---------------------------------------------------------------------------

def get_send_dates(db_conn) -> list[int]:
    """查询 gmdb_plate_info 中所有不同的 send_date。"""
    cur = db_conn.cursor()
    cur.execute("SELECT DISTINCT send_date FROM gmdb_plate_info WHERE send_date IS NOT NULL")
    dates = [r[0] for r in cur.fetchall()]
    cur.close()
    return sorted(dates)


# ---------------------------------------------------------------------------
# 1. 数据库基础设施验证
# ---------------------------------------------------------------------------

class TestDatabaseSetup:
    """验证测试环境就绪。"""

    def test_mysql_connection(self, db_conn):
        """MySQL 可以连接。"""
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        cur.close()

    def test_gmdb_plate_info_exists(self, db_conn):
        """gmdb_plate_info 表有数据。"""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM gmdb_plate_info")
        count = cur.fetchone()[0]
        cur.close()
        assert count > 0

    def test_dqm_tables_created(self, db_conn):
        """DQM 表已创建。"""
        cur = db_conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name IN ('dqm_check_result', 'dqm_security_info_snapshot', 'dqm_accuracy_detail')",
            (TEST_DB_NAME,),
        )
        count = cur.fetchone()[0]
        cur.close()
        assert count == 3

    def test_gmdb_plate_info_send_dates(self, db_conn):
        """查看 gmdb_plate_info 中的 send_date 分布。"""
        dates = get_send_dates(db_conn)
        print(f"send_date values: {dates}")
        assert len(dates) > 0


# ---------------------------------------------------------------------------
# 2. _query_by_mst_type — 真实查询线上表
# ---------------------------------------------------------------------------

class TestQueryByMstType:
    """测试按 mst_type 分组查询当天数据。"""

    def test_query_returns_list(self, checker, db_conn):
        """查询返回列表格式。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date = dates[0]
        results = checker._query_by_mst_type(send_date)
        assert isinstance(results, list)

    def test_query_has_correct_fields(self, checker, db_conn):
        """查询结果包含 mst_type 和 count 字段。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date = dates[0]
        results = checker._query_by_mst_type(send_date)
        if results:
            assert "mst_type" in results[0]
            assert "count" in results[0]

    def test_query_with_nonexistent_date(self, checker):
        """查询不存在的日期返回空列表。"""
        results = checker._query_by_mst_type(20990101)
        assert results == []

    def test_query_with_mst_type_filter(self, checker, db_conn):
        """按 mst_type 过滤查询。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date = dates[0]
        results = checker._query_by_mst_type(send_date, mst_type="INDUSTRY_PLATE_INFO")
        assert isinstance(results, list)
        if results:
            assert results[0]["mst_type"] == "INDUSTRY_PLATE_INFO"


# ---------------------------------------------------------------------------
# 3. _check — 及时性比对逻辑
# ---------------------------------------------------------------------------

class TestCheckWithRealData:
    """测试 _check 方法使用真实数据库。"""

    def test_check_pass_with_existing_date(self, checker, db_conn, check_params):
        """场景：存在当天 send_date 数据 → PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        # 使用数据库中实际存在的日期
        existing_date = dates[0]
        send_date_int = existing_date
        # 将 check_params 的日期改为已有日期
        real_date = date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)
        real_params = (real_date, datetime.now(), 1)

        result = checker._check(*real_params)
        assert result["status"] == CheckResult.PASS
        assert result["total_count"] > 0

    def test_check_fail_with_nonexistent_date(self, checker, check_params):
        """场景：不存在当天 send_date 数据 → FAIL。"""
        # 使用一个不可能存在的日期
        real_date = date(2099, 12, 31)
        real_params = (real_date, datetime.now(), 1)

        result = checker._check(*real_params)
        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0

    def test_check_result_contains_details(self, checker, db_conn):
        """PASS 时结果中包含各 mst_type 的详情。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date_int = dates[0]
        real_date = date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)
        real_params = (real_date, datetime.now(), 1)

        result = checker._check(*real_params)
        if result["status"] == CheckResult.PASS:
            assert len(result["details"]) > 0
            assert "mst_type" in result["details"][0]
            assert "count" in result["details"][0]


# ---------------------------------------------------------------------------
# 4. _record — 结果真实写入 MySQL
# ---------------------------------------------------------------------------

class TestRecordWithRealDB:
    """测试 _record 方法真实写入 MySQL。"""

    def test_record_inserts_into_db(self, checker, db_conn, check_params):
        """写入 PASS 结果到 dqm_check_result。"""
        result = {
            "status": CheckResult.PASS,
            "check_date": str(check_params[0]),
            "send_date": 20260421,
            "total_count": 115,
            "details": [{"mst_type": "INDUSTRY_PLATE_INFO", "count": 85}],
        }
        checker._record(*check_params, result)

        # 查询数据库验证
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2'")
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["monitor_id"] == "M2"
        assert row["dimension"] == "及时性"
        assert row["result"] == "PASS"
        detail = json.loads(row["detail"])
        assert detail["total_count"] == 115

    def test_record_upsert_idempotent(self, checker, db_conn, check_params):
        """先写 PASS 再写 FAIL，验证 upsert 幂等性。"""
        result_pass = {
            "status": CheckResult.PASS,
            "check_date": str(check_params[0]),
            "send_date": 20260421,
            "total_count": 115,
            "details": [],
        }
        checker._record(*check_params, result_pass)

        result_fail = {
            "status": CheckResult.FAIL,
            "check_date": str(check_params[0]),
            "send_date": 20260421,
            "total_count": 0,
            "details": [],
        }
        checker._record(*check_params, result_fail)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2'")
        rows = cur.fetchall()
        cur.close()

        assert len(rows) == 1
        assert rows[0]["result"] == "FAIL"

    def test_record_fail_with_detail(self, checker, db_conn, check_params):
        """写入 FAIL 结果，detail 包含正确信息。"""
        result = {
            "status": CheckResult.FAIL,
            "check_date": str(check_params[0]),
            "send_date": 20260421,
            "total_count": 0,
            "details": [],
        }
        checker._record(*check_params, result)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2'")
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "FAIL"
        detail = json.loads(row["detail"])
        assert detail["total_count"] == 0
        assert detail["send_date"] == 20260421


# ---------------------------------------------------------------------------
# 5. execute 端到端测试
# ---------------------------------------------------------------------------

class TestExecuteEndToEnd:
    """端到端测试：_check → _record → _alert（跳过 _prepare）。"""

    def test_e2e_pass(self, checker, db_conn):
        """端到端：存在当天数据 → PASS。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date_int = dates[0]
        real_date = date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)
        real_params = (real_date, datetime.now(), 1)

        result = checker._check(*real_params)
        checker._record(*real_params, result)
        checker._alert(*real_params, result)

        # 验证数据库
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2'")
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "PASS"

    def test_e2e_fail(self, checker, db_conn):
        """端到端：不存在当天数据 → FAIL。"""
        real_date = date(2099, 12, 31)
        real_params = (real_date, datetime.now(), 1)

        result = checker._check(*real_params)
        checker._record(*real_params, result)
        checker._alert(*real_params, result)

        # 验证数据库
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2' AND check_date = %s", (real_date,))
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "FAIL"


# ---------------------------------------------------------------------------
# 6. Repository 测试
# ---------------------------------------------------------------------------

class TestRepositoryWithRealDB:
    """测试 CheckResultRepository 在真实 MySQL 上的操作。"""

    def test_check_result_upsert_and_query(self, mysql_storage, db_conn, check_params):
        """upsert 并查询 M2 结果。"""
        repo = CheckResultRepository(mysql_storage)

        repo.upsert(
            check_date=check_params[0],
            check_round=1,
            monitor_id="M2",
            dimension="及时性",
            result="PASS",
            detail='{"total_count": 100}',
        )

        results = repo.get_by_date(check_params[0], monitor_id="M2")
        assert len(results) == 1
        assert results[0]["monitor_id"] == "M2"
        assert results[0]["result"] == "PASS"

    def test_check_result_m1_m2_coexist(self, mysql_storage, db_conn, check_params):
        """M1 和 M2 的结果可以共存。"""
        repo = CheckResultRepository(mysql_storage)

        repo.upsert(
            check_date=check_params[0],
            check_round=1,
            monitor_id="M1",
            dimension="完整性",
            result="PASS",
            detail='{}',
        )
        repo.upsert(
            check_date=check_params[0],
            check_round=1,
            monitor_id="M2",
            dimension="及时性",
            result="FAIL",
            detail='{"total_count": 0}',
        )

        m1_results = repo.get_by_date(check_params[0], monitor_id="M1")
        m2_results = repo.get_by_date(check_params[0], monitor_id="M2")

        assert len(m1_results) == 1
        assert len(m2_results) == 1
        assert m1_results[0]["result"] == "PASS"
        assert m2_results[0]["result"] == "FAIL"


# ---------------------------------------------------------------------------
# 7. 完整 execute 流程（模板方法）
# ---------------------------------------------------------------------------

class TestFullExecute:
    """测试完整 execute 模板方法。"""

    def test_execute_full_pass(self, checker, db_conn):
        """完整 execute → PASS（使用真实存在的日期）。"""
        dates = get_send_dates(db_conn)
        if not dates:
            pytest.skip("gmdb_plate_info 中无 send_date 数据")

        send_date_int = dates[0]
        real_date = date(send_date_int // 10000, (send_date_int // 100) % 100, send_date_int % 100)

        checker.execute(check_date=real_date, check_time=datetime.now(), check_round=1)

        # 验证数据库
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2'")
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "PASS"

    def test_execute_full_fail(self, checker, db_conn):
        """完整 execute → FAIL（使用不存在的日期）。"""
        real_date = date(2099, 12, 31)

        checker.execute(check_date=real_date, check_time=datetime.now(), check_round=1)

        # 验证数据库
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT * FROM dqm_check_result WHERE monitor_id = 'M2' AND check_date = %s", (real_date,))
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "FAIL"
