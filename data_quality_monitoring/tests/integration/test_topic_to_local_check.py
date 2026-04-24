"""从 Topic 监听数据到执行检测的全流程集成测试。

流程：
  testdate/ JSON（模拟从 topic 监听并解码后的数据）
    → 解码为快照记录，写入快照表
    → 向线上表插入匹配数据（模拟本地线上数据）
    → 用 topic 快照数据对比本地线上数据
    → 执行完整性(M1/M4)、及时性(M2/M5)、准确性(M3/M6)检查
    → 验证检查结果全部 PASS

前提：不依赖真实 Pulsar 环境，testdate/ 中的 JSON 文件即为模拟的 topic 数据。
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pymysql
import pytest

from config.constants import CheckResult, Dimension, MonitorID
from config.logger_config import setup_logger
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.checkers.timeliness import TimelinessChecker
from src.dqm.checkers.accuracy import AccuracyChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import CheckResultRepository, SnapshotRepository
from src.dqm.storage.schema import ALL_DDL

# ── 数据库配置 ──────────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"

# ── 测试数据目录 ────────────────────────────────────────
# testdate/ 中的 JSON 文件 = 从 topic 监听并解码后的数据
TESTDATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "testdate"

# 线上表名
TABLE_GMDB = "gmdb_plate_info"
TABLE_ADS_FIN = "ads_fin_index_compn_stock_interface_ds"

# 测试日期（与 testdate/ 中 SEND_DATE 一致）
TEST_DATE = date(2026, 4, 24)
TEST_SEND_DATE = 20260424


# ---------------------------------------------------------------------------
# 辅助函数：解码 topic 消息
# ---------------------------------------------------------------------------

def normalize_stkcode(raw_code: str) -> tuple[str, str]:
    """将原始板块代码转换为快照表标准格式。

    BK0477 → stkcode="0477.BK", std_stkcode="0477.BK"
    """
    m = re.match(r'^([A-Za-z]+)(\d+)$', raw_code)
    if m:
        prefix = m.group(1)
        digits = m.group(2).zfill(4)
        stkcode = f"{digits}.{prefix}"
        return stkcode, stkcode
    return raw_code, ""


def decode_topic_messages(messages: list[dict]) -> list[dict]:
    """将 Pulsar topic 解码后的原始消息转换为快照表记录。

    原始消息格式：
    - INDUSTRY_PLATE_INFO: {MSG_TYPE, SEND_DATE, SEND_TIME, CONTENT: [{stockCode, stockName}]}
    - PLATE_STOCKS: {MSG_TYPE, SEND_DATE, SEND_TIME, CONTENT: {item: {...}, stockList: [...]}}
    """
    records = []
    for msg in messages:
        msg_type = msg.get("MSG_TYPE", "")
        send_date = msg.get("SEND_DATE", "")
        content = msg.get("CONTENT", {})

        if msg_type in ("INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"):
            plate_list = content if isinstance(content, list) else []
            for plate in plate_list:
                raw_code = plate.get("stockCode", "")
                stkcode, std_stkcode = normalize_stkcode(raw_code)
                records.append({
                    "stkcode": stkcode,
                    "stkname": plate.get("stockName", ""),
                    "std_stkcode": std_stkcode,
                    "mst_type": msg_type,
                    "compn_stock_code": "",
                    "compn_stock_name": "",
                    "index_name": plate.get("stockName", ""),
                    "send_date": str(send_date),
                })

        elif msg_type == "PLATE_STOCKS":
            item = content.get("item", {}) if isinstance(content, dict) else {}
            stock_list = content.get("stockList", []) if isinstance(content, dict) else []
            raw_plate_code = item.get("plateCode", "")
            plate_name = item.get("plateName", "")
            stkcode, std_stkcode = normalize_stkcode(raw_plate_code)

            for stock in stock_list:
                records.append({
                    "stkcode": stkcode,
                    "stkname": plate_name,
                    "std_stkcode": std_stkcode,
                    "mst_type": "PLATE_STOCKS",
                    "compn_stock_code": stock.get("stockCode", ""),
                    "compn_stock_name": stock.get("stockName", ""),
                    "index_name": plate_name,
                    "send_date": str(send_date),
                })

    return records


def load_testdata() -> list[dict]:
    """加载 testdate/ 目录下所有 JSON 文件（模拟从 topic 监听到的数据）。"""
    messages = []
    for json_file in sorted(TESTDATA_DIR.glob("*.json")):
        with open(json_file, "r", encoding="utf-8") as f:
            messages.append(json.load(f))
    return messages


def insert_online_gmdb(conn, records: list[dict]):
    """根据快照记录向 gmdb_plate_info 插入模拟线上数据（模拟本地线上数据）。"""
    cur = conn.cursor()
    cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))

    plate_records = [
        r for r in records
        if r["mst_type"] in ("INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO")
    ]

    for r in plate_records:
        stkcode_digits = r["stkcode"].split(".")[0] if "." in r["stkcode"] else r["stkcode"]
        cur.execute(
            """INSERT INTO gmdb_plate_info
               (stkcode, stkname, std_stkcode, zhishubankuaileibie, mst_type, send_date, send_time, index_type_name)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE stkname=VALUES(stkname)""",
            (stkcode_digits, r["stkname"], r["std_stkcode"], "行业板块",
             r["mst_type"], TEST_SEND_DATE, 164030, "A股指数"),
        )
    cur.close()


def insert_online_ads_fin(conn, records: list[dict]):
    """根据快照记录向 ads_fin_index_compn_stock_interface_ds 插入模拟线上数据（模拟本地线上数据）。"""
    cur = conn.cursor()
    cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))

    stock_records = [r for r in records if r["mst_type"] == "PLATE_STOCKS"]

    for r in stock_records:
        cur.execute(
            """INSERT INTO ads_fin_index_compn_stock_interface_ds
               (stkcode, compn_stock_code, compn_stock_name, index_name, send_date,
                compn_stock_thscode, valid_from, valid_to, timestamp, send_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE compn_stock_name=VALUES(compn_stock_name)""",
            (r["stkcode"], r["compn_stock_code"], r["compn_stock_name"],
             r["index_name"], TEST_SEND_DATE,
             r["compn_stock_code"], 631123200000, 2524579200000, 631123200000, 165336),
        )
    cur.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def init_logger():
    setup_logger()


@pytest.fixture(scope="module")
def db_conn():
    """模块级别：原始数据库连接。"""
    conn = pymysql.connect(
        host=TEST_DB_HOST, port=TEST_DB_PORT,
        user=TEST_DB_USER, password=TEST_DB_PASSWORD,
        database=TEST_DB_NAME, charset="utf8mb4", autocommit=True,
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


@pytest.fixture
def mysql_storage():
    """创建 MySQLStorage 实例（patch 连接参数）。"""
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
def topic_messages():
    """加载 testdate/ 中的 JSON（模拟从 topic 监听并解码后的数据）。"""
    return load_testdata()


@pytest.fixture
def snapshot_records(topic_messages):
    """将 topic 消息解码为快照记录。"""
    return decode_topic_messages(topic_messages)


@pytest.fixture
def check_params():
    """通用检查参数：(check_date, check_time, check_round)。"""
    return TEST_DATE, datetime(2026, 4, 24, 9, 0, 0), 1


@pytest.fixture
def prepare_data(mysql_storage, db_conn, snapshot_records):
    """数据准备：topic 快照写入 + 本地线上数据插入，测试后清理。"""
    # Step 1: 模拟从 topic 监听到数据 → 解码写入快照表
    snapshot_repo = SnapshotRepository(mysql_storage)
    snapshot_repo.save(TEST_DATE, 1, snapshot_records)

    # Step 2: 模拟本地线上数据（与 topic 数据一致）
    insert_online_gmdb(db_conn, snapshot_records)
    insert_online_ads_fin(db_conn, snapshot_records)

    yield snapshot_records

    # 清理
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.close()


# ===========================================================================
# 全流程测试：从 topic 监听到数据 → 对比本地数据 → M1~M6 检查
# ===========================================================================

class TestTopicToLocalCheck:
    """从 topic 监听数据对比本地线上数据的完整检测流程。

    流程：
      1. 加载 testdate/ JSON（模拟 topic 监听到并解码后的数据）
      2. 解码为快照记录，写入快照表
      3. 向线上表插入匹配数据（模拟本地线上数据）
      4. 用 topic 快照数据对比本地数据，执行 M1~M6 检查
      5. 验证完整性、及时性、准确性全部 PASS
    """

    # ── 完整性检查 ──────────────────────────────────────

    def test_m1_completeness(self, mysql_storage, prepare_data, check_params):
        """M1 完整性：本地 gmdb_plate_info 的 std_stkcode 与 topic 快照一致 → PASS。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1, table=TABLE_GMDB,
            key_field="std_stkcode", mysql_storage=mysql_storage,
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M1 完整性应 PASS | online={result['online_count']}, "
            f"snapshot={result['snapshot_count']}, missing={result['missing']}, extra={result['extra']}"
        )
        assert result["missing"] == [], f"M1 不应有遗漏: {result['missing']}"
        assert result["extra"] == [], f"M1 不应有多余: {result['extra']}"

    def test_m4_completeness(self, mysql_storage, prepare_data, check_params):
        """M4 完整性：本地 ads_fin 的成分股与 topic 快照一致 → PASS。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M4, table=TABLE_ADS_FIN,
            key_field="compn_stock_code", mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M4 完整性应 PASS | online={result['online_count']}, "
            f"snapshot={result['snapshot_count']}, group_details={result['group_details']}"
        )
        assert result["group_details"] == [], f"M4 不应有分组差异: {result['group_details']}"

    # ── 及时性检查 ──────────────────────────────────────

    def test_m2_timeliness(self, mysql_storage, prepare_data, check_params):
        """M2 及时性：本地 gmdb_plate_info 有当天数据 → PASS。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M2, table=TABLE_GMDB, mysql_storage=mysql_storage,
        )

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M2 及时性应 PASS | total_count={result['total_count']}"
        )
        assert result["total_count"] > 0, "M2 应有当天数据"

    def test_m5_timeliness(self, mysql_storage, prepare_data, check_params):
        """M5 及时性：本地 ads_fin_index 有当天数据 → PASS。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M5, table=TABLE_ADS_FIN, mysql_storage=mysql_storage,
        )

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M5 及时性应 PASS | total_count={result['total_count']}"
        )
        assert result["total_count"] > 0, "M5 应有当天数据"

    # ── 准确性检查 ──────────────────────────────────────

    def test_m3_accuracy(self, mysql_storage, prepare_data, check_params):
        """M3 准确性：本地 gmdb_plate_info 字段均合法 → PASS。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M3, table=TABLE_GMDB,
            key_field="stkcode", mysql_storage=mysql_storage,
        )

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M3 准确性应 PASS | total={result['total']}, errors={result['errors']}, "
            f"details={result['details']}"
        )
        assert result["errors"] == 0, f"M3 不应有字段错误: {result['details']}"

    def test_m6_accuracy(self, mysql_storage, prepare_data, check_params):
        """M6 准确性：本地 ads_fin_index 字段均合法 → PASS。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M6, table=TABLE_ADS_FIN,
            key_field="stkcode", mysql_storage=mysql_storage,
        )

        result = checker._check(*check_params)
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

        assert result["status"] == CheckResult.PASS, (
            f"M6 准确性应 PASS | total={result['total']}, errors={result['errors']}, "
            f"details={result['details']}"
        )
        assert result["errors"] == 0, f"M6 不应有字段错误: {result['details']}"

    # ── 全流程验证 ──────────────────────────────────────

    def test_full_pipeline_all_pass(
        self, mysql_storage, db_conn, snapshot_records, check_params, prepare_data
    ):
        """全流程：topic 数据对比本地数据 → M1~M6 全部 PASS，结果正确持久化。"""
        # 验证快照写入成功
        snapshot_repo = SnapshotRepository(mysql_storage)
        saved = snapshot_repo.get_by_date(TEST_DATE)
        assert len(saved) > 0, "快照表应有数据"

        plate_count = sum(1 for r in saved if r.get("mst_type") != "PLATE_STOCKS")
        stock_count = sum(1 for r in saved if r.get("mst_type") == "PLATE_STOCKS")

        # 依次执行 M1~M6（复用 prepare_data 已写入的快照和线上数据）
        results = {}

        # M1 完整性
        m1 = CompletenessChecker(
            monitor_id=MonitorID.M1, table=TABLE_GMDB,
            key_field="std_stkcode", mysql_storage=mysql_storage,
        )
        m1._pulsar_failed = False
        m1._pulsar_error = ""
        results["M1"] = m1._check(*check_params)
        m1._record(*check_params, results["M1"])
        m1._alert(*check_params, results["M1"])

        # M4 完整性
        m4 = CompletenessChecker(
            monitor_id=MonitorID.M4, table=TABLE_ADS_FIN,
            key_field="compn_stock_code", mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        m4._pulsar_failed = False
        m4._pulsar_error = ""
        results["M4"] = m4._check(*check_params)
        m4._record(*check_params, results["M4"])
        m4._alert(*check_params, results["M4"])

        # M2 及时性
        m2 = TimelinessChecker(monitor_id=MonitorID.M2, table=TABLE_GMDB, mysql_storage=mysql_storage)
        results["M2"] = m2._check(*check_params)
        m2._record(*check_params, results["M2"])
        m2._alert(*check_params, results["M2"])

        # M5 及时性
        m5 = TimelinessChecker(monitor_id=MonitorID.M5, table=TABLE_ADS_FIN, mysql_storage=mysql_storage)
        results["M5"] = m5._check(*check_params)
        m5._record(*check_params, results["M5"])
        m5._alert(*check_params, results["M5"])

        # M3 准确性
        m3 = AccuracyChecker(
            monitor_id=MonitorID.M3, table=TABLE_GMDB,
            key_field="stkcode", mysql_storage=mysql_storage,
        )
        results["M3"] = m3._check(*check_params)
        m3._record(*check_params, results["M3"])
        m3._alert(*check_params, results["M3"])

        # M6 准确性
        m6 = AccuracyChecker(
            monitor_id=MonitorID.M6, table=TABLE_ADS_FIN,
            key_field="stkcode", mysql_storage=mysql_storage,
        )
        results["M6"] = m6._check(*check_params)
        m6._record(*check_params, results["M6"])
        m6._alert(*check_params, results["M6"])

        # 验证全部 PASS
        for mid, r in results.items():
            assert r["status"] == CheckResult.PASS, (
                f"[{mid}] 应 PASS | status={r['status']}"
            )

        # 验证结果持久化
        result_repo = CheckResultRepository(mysql_storage)
        all_results = result_repo.get_by_date(TEST_DATE)
        assert len(all_results) == 6, f"应有 6 条检查结果，实际: {len(all_results)}"

        monitor_ids = {r["monitor_id"] for r in all_results}
        assert monitor_ids == {"M1", "M2", "M3", "M4", "M5", "M6"}

        for r in all_results:
            assert r["result"] == "PASS", (
                f"[{r['monitor_id']}][{r['dimension']}] 应为 PASS，实际: {r['result']}"
            )

        # 输出汇总
        print("\n" + "=" * 80)
        print("Topic 监听数据 vs 本地线上数据 — 检查结果汇总")
        print("=" * 80)
        print(f"检查日期: {TEST_DATE}, 检查轮次: 1")
        print(f"Topic 快照: 板块={plate_count} 条, 成分股={stock_count} 条")
        print("-" * 80)
        for r in all_results:
            detail = json.loads(r["detail"]) if r.get("detail") else {}
            extra_info = ""
            if r["dimension"] == Dimension.COMPLETENESS:
                extra_info = (
                    f"online={detail.get('online_count', '?')}, "
                    f"snapshot={detail.get('snapshot_count', '?')}, "
                    f"missing={detail.get('missing', [])}, extra={detail.get('extra', [])}"
                )
            elif r["dimension"] == Dimension.TIMELINESS:
                extra_info = f"total_count={detail.get('total_count', '?')}"
            elif r["dimension"] == Dimension.ACCURACY:
                extra_info = f"total={detail.get('total', '?')}, errors={detail.get('errors', '?')}"
            print(f"  [{r['monitor_id']}] [{r['dimension']}] {r['result']}  {extra_info}")
        print("=" * 80)
