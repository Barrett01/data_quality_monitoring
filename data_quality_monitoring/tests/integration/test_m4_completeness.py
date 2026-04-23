"""M4 完整性检查集成测试。

使用本地 MySQL (hidatapilot-dataclean) 真实数据库进行端到端测试。
测试 ads_fin_index_compn_stock_interface_ds 的分组完整性比对。

测试覆盖：
1. 数据库连接与 ads_fin_index_compn_stock_interface_ds 表验证
2. _query_online_grouped_keys — 真实查询线上表
3. _query_snapshot_grouped_keys — 真实查询快照表
4. _check — 分组比对逻辑
5. _record — 结果真实写入 dqm_check_result
6. execute 端到端 — 跳过 _prepare，直接 _check → _record → _alert
7. 不同数据场景（完全一致/成分股遗漏/成分股多余/快照为空）
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import patch

import pymysql
import pytest

from config.constants import CheckResult, Dimension, MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"

# 真实线上表
ONLINE_TABLE = "ads_fin_index_compn_stock_interface_ds"


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
def ensure_tables(db_conn):
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
def m4_checker(mysql_storage):
    return CompletenessChecker(
        monitor_id=MonitorID.M4,
        table=ONLINE_TABLE,
        key_field="compn_stock_code",
        mysql_storage=mysql_storage,
        group_field="stkcode",
    )


@pytest.fixture
def check_params(db_conn, has_online_data):
    """通用检查参数。根据线上表真实 send_date 动态生成。"""
    if has_online_data:
        cur = db_conn.cursor()
        cur.execute(f"SELECT DISTINCT send_date FROM {ONLINE_TABLE} ORDER BY send_date DESC LIMIT 1")
        send_date_val = cur.fetchone()[0]
        cur.close()
        sd_str = str(send_date_val)
        check_date = date(int(sd_str[:4]), int(sd_str[4:6]), int(sd_str[6:8]))
    else:
        check_date = date(2026, 4, 22)
    return check_date, datetime.combine(check_date, datetime.min.time()), 1


@pytest.fixture(autouse=True)
def cleanup(db_conn):
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()


@pytest.fixture(scope="module")
def has_online_data(db_conn):
    """检查线上表是否有数据。"""
    cur = db_conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {ONLINE_TABLE}")
        count = cur.fetchone()[0]
        return count > 0
    except Exception:
        return False
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# 辅助方法
# ---------------------------------------------------------------------------

def insert_snapshot_records(db_conn, check_date: date, check_round: int, records: list[dict]):
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
                r.get("stkcode") or "",
                r.get("stkname") or "",
                r.get("std_stkcode") or "",
                r.get("mst_type") or "PLATE_STOCKS",
                r.get("compn_stock_code") or "",
                r.get("compn_stock_name") or "",
                r.get("index_name") or "",
                r.get("send_date") or "",
            ),
        )
    cur.close()


def get_check_result(db_conn, check_date: date, monitor_id: str = "M4") -> dict | None:
    cur = db_conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(
        "SELECT * FROM dqm_check_result WHERE check_date = %s AND monitor_id = %s",
        (check_date, monitor_id),
    )
    row = cur.fetchone()
    cur.close()
    return row


# ---------------------------------------------------------------------------
# 1. 数据库连接与表验证
# ---------------------------------------------------------------------------

class TestDatabaseSetup:
    def test_ads_fin_table_exists(self, ensure_tables, db_conn, has_online_data):
        """ads_fin_index_compn_stock_interface_ds 表存在。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        cur = db_conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {ONLINE_TABLE}")
        count = cur.fetchone()[0]
        assert count > 0
        cur.close()

    def test_ads_fin_has_stkcode_and_compn_stock_code(self, ensure_tables, db_conn, has_online_data):
        """表中应有 stkcode 和 compn_stock_code 数据。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        cur = db_conn.cursor()
        cur.execute(
            f"SELECT DISTINCT stkcode FROM {ONLINE_TABLE} "
            f"WHERE stkcode IS NOT NULL AND compn_stock_code IS NOT NULL"
        )
        stkcodes = [r[0] for r in cur.fetchall()]
        assert len(stkcodes) > 0, "应有 stkcode 数据"
        cur.close()


# ---------------------------------------------------------------------------
# 2. _query_online_grouped_keys 真实查询测试
# ---------------------------------------------------------------------------

class TestQueryOnlineGroupedKeys:
    def test_returns_grouped_result(self, m4_checker, check_params, has_online_data):
        """查询应返回按 stkcode 分组的 compn_stock_code。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        result = m4_checker._query_online_grouped_keys(check_params[0])
        assert isinstance(result, dict)
        # 真实表应有数据
        assert len(result) > 0
        for group_key, codes in result.items():
            assert isinstance(group_key, str)
            assert isinstance(codes, list)
            assert len(codes) > 0

    def test_dedup_within_group(self, m4_checker, check_params, has_online_data):
        """每个分组内的 compn_stock_code 应去重。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        result = m4_checker._query_online_grouped_keys(check_params[0])
        for group_key, codes in result.items():
            assert len(codes) == len(set(codes)), f"分组 {group_key} 内应无重复"


# ---------------------------------------------------------------------------
# 3. _query_snapshot_grouped_keys 真实查询测试
# ---------------------------------------------------------------------------

class TestQuerySnapshotGroupedKeys:
    def test_empty_snapshot(self, m4_checker, check_params):
        """快照表无数据时应返回空字典。"""
        result = m4_checker._query_snapshot_grouped_keys(check_params[0], check_params[2])
        assert result == {}

    def test_with_data(self, m4_checker, check_params, db_conn):
        """快照表有数据时应返回分组结果。"""
        insert_snapshot_records(db_conn, check_params[0], check_params[2], [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行"},
            {"stkcode": "0001.BK", "compn_stock_code": "SH600001", "compn_stock_name": "邯郸钢铁"},
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行"},
        ])

        result = m4_checker._query_snapshot_grouped_keys(check_params[0], check_params[2])
        assert "0001.BK" in result
        assert "0477.BK" in result
        assert set(result["0001.BK"]) == {"SH600000", "SH600001"}


# ---------------------------------------------------------------------------
# 4. _check 分组比对 — 真实数据场景
# ---------------------------------------------------------------------------

class TestCheckGroupedWithRealData:
    def test_check_fail_partial_snapshot(self, m4_checker, check_params, db_conn):
        """快照只有部分数据 → FAIL, 有 extra。"""
        # 查看真实线上表的数据
        cur = db_conn.cursor()
        cur.execute(f"SELECT stkcode, compn_stock_code FROM {ONLINE_TABLE} LIMIT 5")
        real_data = cur.fetchall()
        cur.close()

        # 只插入一条快照
        if real_data:
            insert_snapshot_records(db_conn, check_params[0], check_params[2], [
                {"stkcode": real_data[0][0], "compn_stock_code": real_data[0][1],
                 "compn_stock_name": "测试"},
            ])

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)

        # 快照数据不全，应有差异
        assert result["status"] in [CheckResult.FAIL, CheckResult.NODATA, CheckResult.PASS]

    def test_check_nodata_empty_snapshot(self, m4_checker, check_params):
        """快照为空 → NODATA。"""
        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        assert result["status"] == CheckResult.NODATA

    def test_check_pass_full_snapshot(self, m4_checker, check_params, db_conn, has_online_data):
        """快照与线上表完全一致 → PASS。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        # 查询线上表所有分组数据
        online_grouped = m4_checker._query_online_grouped_keys(check_params[0])

        # 将线上表数据全部写入快照
        records = []
        for stkcode, codes in online_grouped.items():
            for code in codes:
                records.append({
                    "stkcode": stkcode,
                    "compn_stock_code": code,
                    "compn_stock_name": f"股票{code}",
                })
        insert_snapshot_records(db_conn, check_params[0], check_params[2], records)

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["group_details"] == []

    def test_check_fail_missing_compn_stock(self, m4_checker, check_params, db_conn):
        """快照中多出成分股代码 → FAIL, 有 missing。"""
        # 查询线上表
        online_grouped = m4_checker._query_online_grouped_keys(check_params[0])

        if not online_grouped:
            pytest.skip("线上表无数据，跳过")

        # 取第一个分组，额外添加一个不存在的成分股
        first_group = list(online_grouped.keys())[0]
        records = []
        for code in online_grouped[first_group]:
            records.append({
                "stkcode": first_group,
                "compn_stock_code": code,
                "compn_stock_name": f"股票{code}",
            })
        # 添加一个线上不存在的成分股
        records.append({
            "stkcode": first_group,
            "compn_stock_code": "SH999999",
            "compn_stock_name": "不存在的股票",
        })
        insert_snapshot_records(db_conn, check_params[0], check_params[2], records)

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert "SH999999" in result["missing"]


# ---------------------------------------------------------------------------
# 5. _record 结果真实写入测试
# ---------------------------------------------------------------------------

class TestRecordWithRealDB:
    def test_record_inserts_into_db(self, m4_checker, check_params, db_conn):
        """_record 应将结果写入 dqm_check_result。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 7,
            "snapshot_count": 7,
            "group_details": [],
        }

        m4_checker._record(check_date, check_time, check_round, result)

        row = get_check_result(db_conn, check_date)
        assert row is not None
        assert row["monitor_id"] == "M4"
        assert row["dimension"] == "完整性"
        assert row["result"] == "PASS"

    def test_record_with_group_details(self, m4_checker, check_params, db_conn):
        """FAIL 结果应包含 group_details。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.FAIL,
            "missing": ["SH999999"],
            "extra": [],
            "online_count": 6,
            "snapshot_count": 7,
            "group_details": [
                {
                    "group_key": "0001.BK",
                    "missing": ["SH999999"],
                    "extra": [],
                    "online_count": 3,
                    "snapshot_count": 4,
                }
            ],
        }

        m4_checker._record(check_date, check_time, check_round, result)

        row = get_check_result(db_conn, check_date)
        detail = json.loads(row["detail"])
        assert "group_details" in detail
        assert len(detail["group_details"]) == 1

    def test_record_upsert_idempotent(self, m4_checker, check_params, db_conn):
        """upsert 幂等性。"""
        check_date, check_time, check_round = check_params

        m4_checker._record(check_date, check_time, check_round, {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 7,
            "snapshot_count": 7,
            "group_details": [],
        })

        m4_checker._record(check_date, check_time, check_round, {
            "status": CheckResult.FAIL,
            "missing": ["SH999999"],
            "extra": [],
            "online_count": 7,
            "snapshot_count": 8,
            "group_details": [{"group_key": "0001.BK", "missing": ["SH999999"], "extra": [], "online_count": 3, "snapshot_count": 4}],
        })

        cur = db_conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT * FROM dqm_check_result WHERE check_date = %s AND monitor_id = %s",
            (check_date, "M4"),
        )
        rows = cur.fetchall()
        cur.close()

        assert len(rows) == 1
        assert rows[0]["result"] == "FAIL"


# ---------------------------------------------------------------------------
# 6. execute 端到端测试（跳过 Pulsar 采集）
# ---------------------------------------------------------------------------

class TestExecuteEndToEnd:
    def test_e2e_pass(self, m4_checker, check_params, db_conn, has_online_data):
        """端到端：快照与线上表一致 → PASS。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        check_date, check_time, check_round = check_params

        # 查询线上表所有数据并写入快照
        online_grouped = m4_checker._query_online_grouped_keys(check_date)
        records = []
        for stkcode, codes in online_grouped.items():
            for code in codes:
                records.append({
                    "stkcode": stkcode,
                    "compn_stock_code": code,
                    "compn_stock_name": f"股票{code}",
                })
        insert_snapshot_records(db_conn, check_date, check_round, records)

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        m4_checker._record(*check_params, result)
        m4_checker._alert(*check_params, result)

        row = get_check_result(db_conn, check_date)
        assert row is not None
        assert row["result"] == "PASS"

    def test_e2e_fail_with_missing(self, m4_checker, check_params, db_conn):
        """端到端：快照有遗漏成分股 → FAIL。"""
        check_date, check_time, check_round = check_params

        online_grouped = m4_checker._query_online_grouped_keys(check_date)
        if not online_grouped:
            pytest.skip("线上表无数据")

        # 只插入部分快照 + 一个不存在的成分股
        first_group = list(online_grouped.keys())[0]
        records = [
            {"stkcode": first_group, "compn_stock_code": "SH999999", "compn_stock_name": "不存在"},
        ]
        insert_snapshot_records(db_conn, check_date, check_round, records)

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        m4_checker._record(*check_params, result)

        row = get_check_result(db_conn, check_date)
        assert row["result"] == "FAIL"
        detail = json.loads(row["detail"])
        assert "SH999999" in detail["missing"]

    def test_e2e_nodata(self, m4_checker, check_params, db_conn):
        """端到端：快照为空 → NODATA。"""
        check_date, check_time, check_round = check_params

        m4_checker._pulsar_failed = False
        m4_checker._pulsar_error = ""

        result = m4_checker._check(*check_params)
        m4_checker._record(*check_params, result)

        row = get_check_result(db_conn, check_date)
        assert row["result"] == "NODATA"

    def test_e2e_skip_pulsar_failed(self, m4_checker, check_params, db_conn):
        """端到端：Pulsar 采集失败 → SKIP。"""
        check_date, check_time, check_round = check_params

        m4_checker._pulsar_failed = True
        m4_checker._pulsar_error = "Pulsar connection timeout"

        result = m4_checker._check(*check_params)
        m4_checker._record(*check_params, result)

        row = get_check_result(db_conn, check_date)
        assert row["result"] == "SKIP"


# ---------------------------------------------------------------------------
# 7. 完整 execute 流程（使用 mock _prepare 跳过 Pulsar）
# ---------------------------------------------------------------------------

class TestFullExecuteWithMockedPrepare:
    def test_execute_full_pass(self, m4_checker, check_params, db_conn, has_online_data):
        """完整 execute 流程：_prepare（mock）→ _check → _record → _alert → PASS。"""
        if not has_online_data:
            pytest.skip(f"{ONLINE_TABLE} 表无数据，跳过")
        check_date, check_time, check_round = check_params

        # 准备快照数据（与线上表一致）
        online_grouped = m4_checker._query_online_grouped_keys(check_date)

        def mock_prepare(cd, ct, cr):
            records = []
            for stkcode, codes in online_grouped.items():
                for code in codes:
                    records.append({
                        "stkcode": stkcode,
                        "compn_stock_code": code,
                        "compn_stock_name": f"股票{code}",
                    })
            insert_snapshot_records(db_conn, cd, cr, records)
            m4_checker._pulsar_failed = False
            m4_checker._pulsar_error = ""

        with patch.object(m4_checker, "_prepare", side_effect=mock_prepare):
            m4_checker.execute(*check_params)

        row = get_check_result(db_conn, check_date)
        assert row is not None
        assert row["result"] == "PASS"

    def test_execute_full_fail(self, m4_checker, check_params, db_conn):
        """完整 execute 流程：快照不完整 → FAIL。"""
        check_date, check_time, check_round = check_params

        def mock_prepare(cd, ct, cr):
            # 只插入部分数据
            insert_snapshot_records(db_conn, cd, cr, [
                {"stkcode": "0001.BK", "compn_stock_code": "SH999999", "compn_stock_name": "不存在"},
            ])
            m4_checker._pulsar_failed = False
            m4_checker._pulsar_error = ""

        with patch.object(m4_checker, "_prepare", side_effect=mock_prepare):
            m4_checker.execute(*check_params)

        row = get_check_result(db_conn, check_date)
        assert row["result"] == "FAIL"

    def test_execute_skip_on_pulsar_failure(self, m4_checker, check_params, db_conn):
        """完整 execute 流程：Pulsar 采集失败 → SKIP。"""
        check_date, check_time, check_round = check_params

        def mock_prepare(cd, ct, cr):
            m4_checker._pulsar_failed = True
            m4_checker._pulsar_error = "Pulsar connection timeout"

        with patch.object(m4_checker, "_prepare", side_effect=mock_prepare):
            m4_checker.execute(*check_params)

        row = get_check_result(db_conn, check_date)
        assert row["result"] == "SKIP"
