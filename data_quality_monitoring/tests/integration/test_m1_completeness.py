"""M1 完整性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
假设数据已落入 dqm_security_info_snapshot，跳过 Pulsar 采集环节。

测试覆盖：
1. 数据库连接与表初始化
2. _query_online_keys — 真实查询 gmdb_plate_info
3. _query_snapshot_keys — 真实查询 dqm_security_info_snapshot
4. _check — 集合比对逻辑（使用真实数据）
5. _record — 结果真实写入 dqm_check_result
6. execute 端到端 — 跳过 _prepare，直接 _check → _record → _alert
7. 不同数据场景（完全一致/有遗漏/有多余/快照为空）
8. upsert 幂等性（同一 check_date+check_round+monitor_id 重复写入）
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import patch, MagicMock

import pymysql
import pytest

from config.constants import CheckResult, Dimension, MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import CheckResultRepository, SnapshotRepository
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
    """模块级别：确保 DQM 监控表存在（使用代码中的 DDL）。

    注意：现有的 dqm_security_info_snapshot 表结构与代码 DDL 不同，
    需要先 DROP 再重建。gmdb_plate_info 是业务表，不动。
    """
    cur = db_conn.cursor()

    # 先删除旧表（如果结构不匹配）
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")

    # 用代码中的 DDL 重建
    for ddl in ALL_DDL:
        cur.execute(ddl)

    cur.close()
    return True


@pytest.fixture
def mysql_storage():
    """每个测试用例：创建 MySQLStorage 实例，测试后关闭。"""
    # 临时覆盖 settings 中的数据库配置
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
    """创建 M1 CompletenessChecker 实例。"""
    return CompletenessChecker(
        monitor_id=MonitorID.M1,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 21), datetime(2026, 4, 21, 9, 0, 0), 1


@pytest.fixture(autouse=True)
def cleanup_check_result(db_conn):
    """每个测试前后清理 dqm_check_result 和 dqm_security_info_snapshot 中的测试数据。"""
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()
    yield
    # 测试后也清理
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()


# ---------------------------------------------------------------------------
# 辅助方法
# ---------------------------------------------------------------------------

def insert_snapshot_records(db_conn, check_date: date, check_round: int, records: list[dict]):
    """向 dqm_security_info_snapshot 插入测试数据。"""
    cur = db_conn.cursor()
    for r in records:
        cur.execute(
            """INSERT INTO dqm_security_info_snapshot
               (check_date, check_round, check_time, stkcode, stkname, std_stkcode, mst_type,
                compn_stock_code, compn_stock_name, index_name, send_date)
               VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)""",
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
            ),
        )
    cur.close()


# ---------------------------------------------------------------------------
# 1. 数据库连接与表初始化测试
# ---------------------------------------------------------------------------

class TestDatabaseSetup:
    """验证数据库连接和表结构。"""

    def test_mysql_connection(self, ensure_tables, db_conn):
        """验证能连接到测试数据库。"""
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        cur.close()

    def test_gmdb_plate_info_exists(self, ensure_tables, db_conn):
        """验证 gmdb_plate_info 业务表存在且有数据。"""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM gmdb_plate_info")
        count = cur.fetchone()[0]
        assert count > 0, "gmdb_plate_info 表应有数据"
        cur.close()

    def test_dqm_tables_created(self, ensure_tables, db_conn):
        """验证 DQM 监控表已创建。"""
        cur = db_conn.cursor()
        for table in ["dqm_security_info_snapshot", "dqm_check_result", "dqm_accuracy_detail"]:
            cur.execute(f"SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                        (TEST_DB_NAME, table))
            assert cur.fetchone() is not None, f"{table} 表应存在"
        cur.close()

    def test_gmdb_plate_info_stkcodes(self, ensure_tables, db_conn):
        """查看 gmdb_plate_info 中的去重 stkcode 集合。"""
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT stkcode FROM gmdb_plate_info WHERE stkcode IS NOT NULL AND stkcode != ''")
        stkcodes = sorted([r[0] for r in cur.fetchall()])
        print(f"\ngmdb_plate_info stkcodes: {stkcodes}")
        assert len(stkcodes) > 0
        cur.close()


# ---------------------------------------------------------------------------
# 2. _query_online_keys 真实查询测试
# ---------------------------------------------------------------------------

class TestQueryOnlineKeys:
    """使用真实 MySQL 查询 gmdb_plate_info。"""

    def test_query_returns_distinct_stkcodes(self, checker, check_params):
        """查询应返回去重的 stkcode 列表。"""
        result = checker._query_online_keys(check_params[0])
        assert isinstance(result, list)
        assert len(result) > 0
        # 验证无重复
        assert len(result) == len(set(result)), "返回的 stkcode 应去重"

    def test_query_no_empty_values(self, checker, check_params):
        """查询结果不应包含空值。"""
        result = checker._query_online_keys(check_params[0])
        for stkcode in result:
            assert stkcode, "不应包含空值"


# ---------------------------------------------------------------------------
# 3. _query_snapshot_keys 真实查询测试
# ---------------------------------------------------------------------------

class TestQuerySnapshotKeys:
    """使用真实 MySQL 查询 dqm_security_info_snapshot。"""

    def test_query_empty_snapshot(self, checker, check_params):
        """快照表无数据时应返回空列表。"""
        result = checker._query_snapshot_keys(check_params[0], check_params[2])
        assert result == []

    def test_query_with_data(self, checker, check_params, db_conn):
        """快照表有数据时应返回对应的 stkcode。"""
        insert_snapshot_records(db_conn, check_params[0], check_params[2], [
            {"stkcode": "0477", "stkname": "酿酒行业", "mst_type": "INDUSTRY_PLATE_INFO"},
            {"stkcode": "0438", "stkname": "医药制造", "mst_type": "INDUSTRY_PLATE_INFO"},
        ])

        result = checker._query_snapshot_keys(check_params[0], check_params[2])
        assert sorted(result) == ["0438", "0477"]


# ---------------------------------------------------------------------------
# 4. _check 集合比对 — 真实数据场景
# ---------------------------------------------------------------------------

class TestCheckWithRealData:
    """使用真实 MySQL 数据进行集合比对。"""

    def test_check_fail_snapshot_subset_of_online(self, checker, check_params, db_conn):
        """场景：快照是线上表的子集 → FAIL, 有 extra。

        gmdb_plate_info 有 {'0424','0438','0439','0443','0447','0477'}
        快照只有 {'0438','0477'}
        → missing=[], extra={'0424','0439','0443','0447'}
        """
        insert_snapshot_records(db_conn, check_params[0], check_params[2], [
            {"stkcode": "0477", "stkname": "酿酒行业", "mst_type": "INDUSTRY_PLATE_INFO"},
            {"stkcode": "0438", "stkname": "医药制造", "mst_type": "INDUSTRY_PLATE_INFO"},
        ])

        result = checker._check(*check_params)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.FAIL
        assert result["missing"] == []
        assert len(result["extra"]) > 0, "线上表应有快照中不存在的 stkcode"

    def test_check_fail_online_missing_some(self, checker, check_params, db_conn):
        """场景：线上表缺少快照中的某些代码 → FAIL, 有 missing。"""
        # 插入一个 gmdb_plate_info 中不存在的 stkcode
        insert_snapshot_records(db_conn, check_params[0], check_params[2], [
            {"stkcode": "0477", "stkname": "酿酒行业", "mst_type": "INDUSTRY_PLATE_INFO"},
            {"stkcode": "9999", "stkname": "不存在板块", "mst_type": "INDUSTRY_PLATE_INFO"},
        ])

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert "9999" in result["missing"]

    def test_check_nodata_empty_snapshot(self, checker, check_params):
        """场景：快照为空 → NODATA。"""
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.NODATA

    def test_check_pass_all_matched(self, checker, check_params, db_conn):
        """场景：快照与线上表完全一致 → PASS。"""
        # 查询线上表所有 stkcode
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT stkcode FROM gmdb_plate_info WHERE stkcode IS NOT NULL AND stkcode != ''")
        online_stkcodes = sorted([r[0] for r in cur.fetchall()])
        cur.close()

        # 将线上表的所有 stkcode 写入快照
        records = [
            {"stkcode": s, "stkname": f"板块{s}", "mst_type": "INDUSTRY_PLATE_INFO"}
            for s in online_stkcodes
        ]
        insert_snapshot_records(db_conn, check_params[0], check_params[2], records)

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.PASS
        assert result["missing"] == []
        assert result["extra"] == []
        assert result["online_count"] == result["snapshot_count"]


# ---------------------------------------------------------------------------
# 5. _record 结果真实写入测试
# ---------------------------------------------------------------------------

class TestRecordWithRealDB:
    """检查结果真实写入 MySQL 测试。"""

    def test_record_inserts_into_db(self, checker, check_params, db_conn):
        """_record 应将结果写入 dqm_check_result。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 6,
            "snapshot_count": 6,
        }

        checker._record(check_date, check_time, check_round, result)

        # 查询数据库验证
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row is not None, "检查结果应已写入"
        assert row["monitor_id"] == "M1"
        assert row["dimension"] == "完整性"
        assert row["result"] == "PASS"
        detail = json.loads(row["detail"])
        assert detail["online_count"] == 6
        assert detail["snapshot_count"] == 6

    def test_record_upsert_idempotent(self, checker, check_params, db_conn):
        """同一 check_date+check_round+monitor_id 重复写入应覆盖（upsert 幂等性）。"""
        check_date, check_time, check_round = check_params

        # 第一次写入 PASS
        checker._record(check_date, check_time, check_round, {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 6,
            "snapshot_count": 6,
        })

        # 第二次写入 FAIL（覆盖）
        checker._record(check_date, check_time, check_round, {
            "status": CheckResult.FAIL,
            "missing": ["9999"],
            "extra": ["0424"],
            "online_count": 6,
            "snapshot_count": 5,
        })

        # 验证只有一条记录且为 FAIL
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        rows = cur.fetchall()
        cur.close()

        assert len(rows) == 1, "upsert 应只有一条记录"
        assert rows[0]["result"] == "FAIL"

    def test_record_fail_with_detail(self, checker, check_params, db_conn):
        """FAIL 结果应包含 missing 和 extra 详情。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.FAIL,
            "missing": ["9999", "8888"],
            "extra": ["0424"],
            "online_count": 7,
            "snapshot_count": 8,
        }

        checker._record(check_date, check_time, check_round, result)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT detail FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        detail = json.loads(row["detail"])
        assert "9999" in detail["missing"]
        assert "0424" in detail["extra"]


# ---------------------------------------------------------------------------
# 6. execute 端到端测试（跳过 Pulsar 采集）
# ---------------------------------------------------------------------------

class TestExecuteEndToEnd:
    """端到端测试：跳过 _prepare（Pulsar），手动准备快照数据后执行完整流程。"""

    def test_e2e_pass(self, checker, check_params, db_conn):
        """端到端：快照与线上表一致 → 完整流程 PASS。"""
        check_date, check_time, check_round = check_params

        # 手动插入与线上表一致的快照数据
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT stkcode FROM gmdb_plate_info WHERE stkcode IS NOT NULL AND stkcode != ''")
        online_stkcodes = [r[0] for r in cur.fetchall()]
        cur.close()

        records = [
            {"stkcode": s, "stkname": f"板块{s}", "mst_type": "INDUSTRY_PLATE_INFO"}
            for s in online_stkcodes
        ]
        insert_snapshot_records(db_conn, check_date, check_round, records)

        # 标记 Pulsar 未失败，跳过 _prepare
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        # 手动调用 _check → _record → _alert（跳过 _prepare）
        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        # 验证数据库中写入了 PASS
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "PASS"

    def test_e2e_fail_with_missing_and_extra(self, checker, check_params, db_conn):
        """端到端：快照与线上表不一致 → FAIL，结果含 missing 和 extra。"""
        check_date, check_time, check_round = check_params

        # 只插入部分快照 + 一个线上表不存在的代码
        insert_snapshot_records(db_conn, check_date, check_round, [
            {"stkcode": "0477", "stkname": "酿酒行业", "mst_type": "INDUSTRY_PLATE_INFO"},
            {"stkcode": "9999", "stkname": "不存在", "mst_type": "INDUSTRY_PLATE_INFO"},
        ])

        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        checker._record(*check_params, result)

        # 验证结果
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "FAIL"
        detail = json.loads(row["detail"])
        assert "9999" in detail["missing"]
        assert len(detail["extra"]) > 0

    def test_e2e_nodata(self, checker, check_params, db_conn):
        """端到端：快照为空 → NODATA。"""
        check_date, check_time, check_round = check_params

        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        checker._record(*check_params, result)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "NODATA"

    def test_e2e_skip_pulsar_failed(self, checker, check_params, db_conn):
        """端到端：Pulsar 采集失败 → SKIP。"""
        check_date, check_time, check_round = check_params

        checker._pulsar_failed = True
        checker._pulsar_error = "Connection refused"

        result = checker._check(*check_params)
        checker._record(*check_params, result)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "SKIP"


# ---------------------------------------------------------------------------
# 7. Repository 层真实操作测试
# ---------------------------------------------------------------------------

class TestRepositoryWithRealDB:
    """Repository 数据访问层真实数据库操作测试。"""

    def test_snapshot_save_and_query(self, mysql_storage, check_params, db_conn):
        """SnapshotRepository.save 应先删后插，get_by_date 应能查到。"""
        repo = SnapshotRepository(mysql_storage)
        check_date, _, check_round = check_params

        records = [
            {"stkcode": "0477", "stkname": "酿酒行业", "std_stkcode": "0477.BK",
             "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": None,
             "compn_stock_name": None, "index_name": None, "send_date": "20260417"},
        ]
        repo.save(check_date, check_round, records)

        rows = repo.get_by_date(check_date, check_round)
        assert len(rows) == 1
        assert rows[0]["stkcode"] == "0477"

    def test_snapshot_save_overwrites(self, mysql_storage, check_params, db_conn):
        """SnapshotRepository.save 应先删后插（幂等）。"""
        repo = SnapshotRepository(mysql_storage)
        check_date, _, check_round = check_params

        records1 = [
            {"stkcode": "0477", "stkname": "酿酒行业", "std_stkcode": "0477.BK",
             "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": None,
             "compn_stock_name": None, "index_name": None, "send_date": "20260417"},
        ]
        repo.save(check_date, check_round, records1)

        records2 = [
            {"stkcode": "0438", "stkname": "医药制造", "std_stkcode": "0438.BK",
             "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": None,
             "compn_stock_name": None, "index_name": None, "send_date": "20260417"},
            {"stkcode": "0447", "stkname": "电子元件", "std_stkcode": "0447.BK",
             "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": None,
             "compn_stock_name": None, "index_name": None, "send_date": "20260417"},
        ]
        repo.save(check_date, check_round, records2)

        rows = repo.get_by_date(check_date, check_round)
        assert len(rows) == 2
        stkcodes = sorted([r["stkcode"] for r in rows])
        assert stkcodes == ["0438", "0447"]

    def test_check_result_upsert_and_query(self, mysql_storage, check_params):
        """CheckResultRepository.upsert 应能插入和覆盖。"""
        repo = CheckResultRepository(mysql_storage)
        check_date, _, check_round = check_params

        # 第一次插入
        repo.upsert(check_date, check_round, "M1", "完整性", "PASS", '{"missing":[]}')
        rows = repo.get_by_date(check_date, "M1")
        assert len(rows) == 1
        assert rows[0]["result"] == "PASS"

        # 覆盖
        repo.upsert(check_date, check_round, "M1", "完整性", "FAIL", '{"missing":["9999"]}')
        rows = repo.get_by_date(check_date, "M1")
        assert len(rows) == 1
        assert rows[0]["result"] == "FAIL"

    def test_check_result_multiple_monitors(self, mysql_storage, check_params):
        """不同 monitor_id 应独立存储。"""
        repo = CheckResultRepository(mysql_storage)
        check_date, _, check_round = check_params

        repo.upsert(check_date, check_round, "M1", "完整性", "PASS", '{}')
        repo.upsert(check_date, check_round, "M2", "及时性", "FAIL", '{}')

        rows = repo.get_by_date(check_date)
        assert len(rows) == 2

        m1_rows = repo.get_by_date(check_date, "M1")
        assert len(m1_rows) == 1
        assert m1_rows[0]["monitor_id"] == "M1"


# ---------------------------------------------------------------------------
# 8. 完整 execute 流程（使用 mock _prepare 跳过 Pulsar）
# ---------------------------------------------------------------------------

class TestFullExecuteWithMockedPrepare:
    """使用 mock _prepare 跳过 Pulsar 采集，测试完整的 execute 模板方法流程。"""

    def test_execute_full_pass(self, checker, check_params, db_conn):
        """完整 execute 流程：_prepare（mock）→ _check → _record → _alert → PASS。"""
        check_date, check_time, check_round = check_params

        # 准备快照数据（与线上表一致）
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT stkcode FROM gmdb_plate_info WHERE stkcode IS NOT NULL AND stkcode != ''")
        online_stkcodes = [r[0] for r in cur.fetchall()]
        cur.close()

        def mock_prepare(cd, ct, cr):
            """模拟 _prepare：直接往快照表插入数据。"""
            records = [
                {"stkcode": s, "stkname": f"板块{s}", "mst_type": "INDUSTRY_PLATE_INFO"}
                for s in online_stkcodes
            ]
            insert_snapshot_records(db_conn, cd, cr, records)
            checker._pulsar_failed = False
            checker._pulsar_error = ""

        with patch.object(checker, "_prepare", side_effect=mock_prepare):
            checker.execute(*check_params)

        # 验证结果
        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row is not None
        assert row["result"] == "PASS"

    def test_execute_full_fail(self, checker, check_params, db_conn):
        """完整 execute 流程：快照不完整 → FAIL。"""
        check_date, check_time, check_round = check_params

        def mock_prepare(cd, ct, cr):
            """模拟 _prepare：只插入部分数据。"""
            insert_snapshot_records(db_conn, cd, cr, [
                {"stkcode": "0477", "stkname": "酿酒行业", "mst_type": "INDUSTRY_PLATE_INFO"},
            ])
            checker._pulsar_failed = False
            checker._pulsar_error = ""

        with patch.object(checker, "_prepare", side_effect=mock_prepare):
            checker.execute(*check_params)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "FAIL"

    def test_execute_skip_on_pulsar_failure(self, checker, check_params, db_conn):
        """完整 execute 流程：Pulsar 采集失败 → SKIP。"""
        check_date, check_time, check_round = check_params

        def mock_prepare(cd, ct, cr):
            """模拟 Pulsar 采集失败。"""
            checker._pulsar_failed = True
            checker._pulsar_error = "Pulsar connection timeout"

        with patch.object(checker, "_prepare", side_effect=mock_prepare):
            checker.execute(*check_params)

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = %s AND monitor_id = %s",
            (check_date, check_round, "M1"),
        )
        row = cur.fetchone()
        cur.close()

        assert row["result"] == "SKIP"
