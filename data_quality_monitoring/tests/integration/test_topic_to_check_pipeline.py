"""从 Topic 监听到数据质量检测全流程集成测试。

模拟场景：
1. 读取 testdate/ 中的 JSON 文件 → 模拟从 Pulsar topic 监听并解码后的数据
2. 将解码后的数据写入快照表 dqm_security_info_snapshot → 模拟 _prepare 阶段
3. 向线上表（gmdb_plate_info、ads_fin_index_compn_stock_interface_ds）插入模拟数据 → 模拟线上数据
4. 用快照数据对比线上数据，执行完整性（M1/M4）、及时性（M2/M5）、准确性（M3/M6）检查
5. 验证检查结果：当线上数据与快照完全一致时，全部 PASS

关键设计：
- 不依赖真实 Pulsar 环境，用 testdate/ 中的 JSON 文件模拟
- 不依赖线上表中已有的数据，测试开始前插入匹配的模拟数据，测试结束后清理
- 可预测的测试结果：线上数据与 topic 数据一致 → PASS；人为制造差异 → FAIL
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

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"

# ── 测试数据目录 ──────────────────────────────────────
TESTDATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "testdate"

# 线上表名
TABLE_GMDB = "gmdb_plate_info"
TABLE_ADS_FIN = "ads_fin_index_compn_stock_interface_ds"

# 测试日期
TEST_DATE = date(2026, 4, 24)
TEST_SEND_DATE = 20260424


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def normalize_stkcode(raw_code: str) -> tuple[str, str]:
    """将原始板块代码转换为快照表标准格式。

    转换规则：将前缀字母移到末尾，用 '.' 连接。
    数字部分左补零到4位，以满足准确性检查的正则约束。
      BK0477 → stkcode="0477.BK", std_stkcode="0477.BK"
      BL5088 → stkcode="5088.BL", std_stkcode="5088.BL"

    这样快照表的 std_stkcode 与线上表的 std_stkcode 格式一致，M1 完整性比对可匹配。
    """
    m = re.match(r'^([A-Za-z]+)(\d+)$', raw_code)
    if m:
        prefix = m.group(1)
        digits = m.group(2).zfill(4)  # 左补零到4位
        stkcode = f"{digits}.{prefix}"
        return stkcode, stkcode
    return raw_code, ""


def decode_topic_messages(messages: list[dict]) -> list[dict]:
    """将 Pulsar topic 解码后的原始消息转换为快照表记录。"""
    records = []
    for msg in messages:
        msg_type = msg.get("MSG_TYPE", "")
        send_date = msg.get("SEND_DATE", "")
        content = msg.get("CONTENT", {})

        if msg_type == "INDUSTRY_PLATE_INFO":
            plate_list = content if isinstance(content, list) else []
            for plate in plate_list:
                raw_code = plate.get("stockCode", "")
                stkcode, std_stkcode = normalize_stkcode(raw_code)
                records.append({
                    "stkcode": stkcode,
                    "stkname": plate.get("stockName", ""),
                    "std_stkcode": std_stkcode,
                    "mst_type": "INDUSTRY_PLATE_INFO",
                    "compn_stock_code": "",
                    "compn_stock_name": "",
                    "index_name": plate.get("stockName", ""),
                    "send_date": str(send_date),
                })

        elif msg_type in ("REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"):
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
    """加载 testdate/ 目录下所有 JSON 文件作为模拟的 topic 消息。"""
    messages = []
    for json_file in sorted(TESTDATA_DIR.glob("*.json")):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            messages.append(data)
    return messages


def insert_online_gmdb(conn, records: list[dict], *, exclude_stkcode: str | None = None):
    """根据快照记录向 gmdb_plate_info 插入模拟线上数据。

    只处理板块类型（INDUSTRY_PLATE_INFO / REGION_PLATE_INFO / HOTIDEA_PLATE_INFO）的记录。
    线上表 stkcode / std_stkcode 格式与快照表一致（4位数字.2位字母）。

    Args:
        exclude_stkcode: 可选，排除指定 std_stkcode 的记录不插入（制造完整性差异）
    """
    cur = conn.cursor()
    # 清理当天旧数据
    cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))

    plate_records = [
        r for r in records
        if r["mst_type"] in ("INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO")
        and r.get("std_stkcode") != exclude_stkcode
    ]

    for r in plate_records:
        # 线上表 stkcode: 取数字部分（与快照一致，4位左补零）
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


def insert_online_ads_fin(conn, records: list[dict], *, exclude_compn_code: str | None = None):
    """根据快照记录向 ads_fin_index_compn_stock_interface_ds 插入模拟线上数据。

    只处理 PLATE_STOCKS 类型的记录。
    注意：该表没有 mst_type 字段，所有数据即 PLATE_STOCKS 类型。

    Args:
        exclude_compn_code: 可选，排除指定 compn_stock_code 的记录不插入（制造完整性差异）
    """
    cur = conn.cursor()
    # 清理当天旧数据
    cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))

    stock_records = [
        r for r in records
        if r["mst_type"] == "PLATE_STOCKS"
        and r.get("compn_stock_code") != exclude_compn_code
    ]

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
def topic_messages():
    """加载模拟的 topic 消息。"""
    return load_testdata()


@pytest.fixture
def snapshot_records(topic_messages):
    """将 topic 消息解码为快照记录。"""
    return decode_topic_messages(topic_messages)


@pytest.fixture
def check_params():
    """通用检查参数：check_date, check_time, check_round。"""
    return TEST_DATE, datetime(2026, 4, 24, 9, 0, 0), 1


@pytest.fixture
def clean_all(db_conn):
    """每个测试前后：清理快照表 + 检查结果表 + 准确性明细表。"""
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()


@pytest.fixture
def prepare_matched_data(db_conn, snapshot_records, clean_all):
    """准备"线上数据与 topic 数据完全一致"的场景。

    1. 将 topic 解码后的数据写入快照表
    2. 向线上表插入与快照匹配的数据
    """
    # 写入快照表
    storage = MySQLStorage()
    try:
        snapshot_repo = SnapshotRepository(storage)
        snapshot_repo.save(TEST_DATE, 1, snapshot_records)
    finally:
        storage.close()

    # 向线上表插入匹配数据
    insert_online_gmdb(db_conn, snapshot_records)
    insert_online_ads_fin(db_conn, snapshot_records)

    return snapshot_records


# ===========================================================================
# 测试类 1：数据完全一致 → 全部 PASS
# ===========================================================================

class TestAllPass:
    """场景：线上数据与 topic 快照数据完全一致，所有检查项应 PASS。"""

    def test_m1_completeness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M1 完整性：线上表 gmdb_plate_info 的 std_stkcode 与快照完全一致 → PASS。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table=TABLE_GMDB,
            key_field="std_stkcode",
            mysql_storage=mysql_storage,
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M1 应 PASS | online={result['online_count']}, snapshot={result['snapshot_count']}, "
            f"missing={result['missing']}, extra={result['extra']}"
        )
        assert result["missing"] == [], f"M1 不应有遗漏: {result['missing']}"
        assert result["extra"] == [], f"M1 不应有多余: {result['extra']}"

    def test_m4_completeness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M4 完整性：线上表 ads_fin 的 compn_stock_code 与快照完全一致 → PASS。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M4,
            table=TABLE_ADS_FIN,
            key_field="compn_stock_code",
            mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M4 应 PASS | online={result['online_count']}, snapshot={result['snapshot_count']}, "
            f"group_details={result['group_details']}"
        )
        assert result["group_details"] == [], f"M4 不应有分组差异: {result['group_details']}"

    def test_m2_timeliness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M2 及时性：gmdb_plate_info 有当天数据 → PASS。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M2,
            table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M2 应 PASS | total_count={result['total_count']}"
        )
        assert result["total_count"] > 0

    def test_m5_timeliness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M5 及时性：ads_fin_index 有当天数据 → PASS。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M5,
            table=TABLE_ADS_FIN,
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M5 应 PASS | total_count={result['total_count']}"
        )
        assert result["total_count"] > 0

    def test_m3_accuracy_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M3 准确性：gmdb_plate_info 字段均合法 → PASS。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M3,
            table=TABLE_GMDB,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M3 应 PASS | total={result['total']}, errors={result['errors']}, "
            f"details={result['details']}"
        )
        assert result["errors"] == 0

    def test_m6_accuracy_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M6 准确性：ads_fin_index 字段均合法 → PASS。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M6,
            table=TABLE_ADS_FIN,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, (
            f"M6 应 PASS | total={result['total']}, errors={result['errors']}, "
            f"details={result['details']}"
        )
        assert result["errors"] == 0


# ===========================================================================
# 测试类 2：完整性差异场景
# ===========================================================================

class TestCompletenessMismatch:
    """场景：线上表缺少部分数据 → M1/M4 应 FAIL。"""

    @pytest.fixture
    def prepare_missing_data(self, db_conn, snapshot_records, clean_all):
        """线上表故意少插入一条板块数据，制造完整性差异。"""
        # 写入快照表（完整数据）
        storage = MySQLStorage()
        try:
            snapshot_repo = SnapshotRepository(storage)
            snapshot_repo.save(TEST_DATE, 1, snapshot_records)
        finally:
            storage.close()

        # 使用 insert_online_gmdb 的 exclude_stkcode 参数排除 "0447.BK"
        insert_online_gmdb(db_conn, snapshot_records, exclude_stkcode="0447.BK")

        # ads_fin 插入完整数据（M4 不受影响）
        insert_online_ads_fin(db_conn, snapshot_records)

        return snapshot_records

    def test_m1_missing_in_online(self, mysql_storage, prepare_missing_data, check_params):
        """M1 完整性：线上少一条板块 → FAIL，missing 包含 '0447.BK'。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table=TABLE_GMDB,
            key_field="std_stkcode",
            mysql_storage=mysql_storage,
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL, "M1 应 FAIL（线上缺少 0447.BK）"
        assert "0447.BK" in result["missing"], f"missing 应包含 '0447.BK': {result['missing']}"
        assert result["snapshot_count"] > result["online_count"]

    def test_m4_not_affected(self, mysql_storage, prepare_missing_data, check_params):
        """M4 完整性：ads_fin 数据完整 → 仍 PASS。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M4,
            table=TABLE_ADS_FIN,
            key_field="compn_stock_code",
            mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        checker._pulsar_failed = False
        checker._pulsar_error = ""

        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS, "M4 应 PASS（ads_fin 数据完整）"


# ===========================================================================
# 测试类 3：及时性缺失场景
# ===========================================================================

class TestTimelinessMissing:
    """场景：线上表无当天数据 → M2/M5 应 FAIL。"""

    @pytest.fixture
    def prepare_no_today_data(self, db_conn, snapshot_records, clean_all):
        """线上表无当天数据。"""
        # 写入快照表
        storage = MySQLStorage()
        try:
            snapshot_repo = SnapshotRepository(storage)
            snapshot_repo.save(TEST_DATE, 1, snapshot_records)
        finally:
            storage.close()

        # 清理线上表当天数据
        cur = db_conn.cursor()
        cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))
        cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))
        cur.close()

        return snapshot_records

    def test_m2_no_data_fail(self, mysql_storage, prepare_no_today_data, check_params):
        """M2 及时性：gmdb_plate_info 无当天数据 → FAIL。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M2,
            table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL, "M2 应 FAIL（无当天数据）"
        assert result["total_count"] == 0

    def test_m5_no_data_fail(self, mysql_storage, prepare_no_today_data, check_params):
        """M5 及时性：ads_fin_index 无当天数据 → FAIL。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M5,
            table=TABLE_ADS_FIN,
            mysql_storage=mysql_storage,
        )
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL, "M5 应 FAIL（无当天数据）"
        assert result["total_count"] == 0


# ===========================================================================
# 测试类 4：全流程端到端（M1~M6 顺序执行）
# ===========================================================================

class TestFullTopicToCheckPipeline:
    """端到端全流程：topic 解码 → 快照写入 → M1~M6 全部检查 → 结果验证 → 结果持久化。"""

    def test_full_pipeline_matched(
        self, mysql_storage, db_conn, snapshot_records, check_params, clean_all
    ):
        """完整流程：线上数据与 topic 一致 → M1~M6 全部 PASS。"""
        # ── Step 1: topic 消息解码 → 写入快照表 ──
        snapshot_repo = SnapshotRepository(mysql_storage)
        snapshot_repo.save(TEST_DATE, 1, snapshot_records)

        saved = snapshot_repo.get_by_date(TEST_DATE)
        assert len(saved) > 0, "快照表应有数据"

        # ── Step 2: 向线上表插入匹配数据 ──
        insert_online_gmdb(db_conn, snapshot_records)
        insert_online_ads_fin(db_conn, snapshot_records)

        # ── Step 3: M1 完整性 ──
        m1 = CompletenessChecker(
            monitor_id=MonitorID.M1, table=TABLE_GMDB,
            key_field="std_stkcode", mysql_storage=mysql_storage,
        )
        m1._pulsar_failed = False
        m1._pulsar_error = ""
        m1_result = m1._check(*check_params)
        assert m1_result["status"] == CheckResult.PASS
        m1._record(*check_params, m1_result)
        m1._alert(*check_params, m1_result)

        # ── Step 4: M4 完整性 ──
        m4 = CompletenessChecker(
            monitor_id=MonitorID.M4, table=TABLE_ADS_FIN,
            key_field="compn_stock_code", mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        m4._pulsar_failed = False
        m4._pulsar_error = ""
        m4_result = m4._check(*check_params)
        assert m4_result["status"] == CheckResult.PASS
        m4._record(*check_params, m4_result)
        m4._alert(*check_params, m4_result)

        # ── Step 5: M2 及时性 ──
        m2 = TimelinessChecker(
            monitor_id=MonitorID.M2, table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )
        m2_result = m2._check(*check_params)
        assert m2_result["status"] == CheckResult.PASS
        m2._record(*check_params, m2_result)
        m2._alert(*check_params, m2_result)

        # ── Step 6: M5 及时性 ──
        m5 = TimelinessChecker(
            monitor_id=MonitorID.M5, table=TABLE_ADS_FIN,
            mysql_storage=mysql_storage,
        )
        m5_result = m5._check(*check_params)
        assert m5_result["status"] == CheckResult.PASS
        m5._record(*check_params, m5_result)
        m5._alert(*check_params, m5_result)

        # ── Step 7: M3 准确性 ──
        m3 = AccuracyChecker(
            monitor_id=MonitorID.M3, table=TABLE_GMDB,
            key_field="stkcode", mysql_storage=mysql_storage,
        )
        m3_result = m3._check(*check_params)
        assert m3_result["status"] == CheckResult.PASS
        m3._record(*check_params, m3_result)
        m3._alert(*check_params, m3_result)

        # ── Step 8: M6 准确性 ──
        m6 = AccuracyChecker(
            monitor_id=MonitorID.M6, table=TABLE_ADS_FIN,
            key_field="stkcode", mysql_storage=mysql_storage,
        )
        m6_result = m6._check(*check_params)
        assert m6_result["status"] == CheckResult.PASS
        m6._record(*check_params, m6_result)
        m6._alert(*check_params, m6_result)

        # ── Step 9: 验证结果持久化 ──
        result_repo = CheckResultRepository(mysql_storage)
        all_results = result_repo.get_by_date(TEST_DATE)
        assert len(all_results) == 6, f"应有 6 条检查结果，实际: {len(all_results)}"

        # 验证全部 PASS
        for r in all_results:
            assert r["result"] == "PASS", (
                f"[{r['monitor_id']}][{r['dimension']}] 应为 PASS，实际: {r['result']}"
            )

        # ── Step 10: 输出汇总 ──
        print("\n" + "=" * 80)
        print("Topic → 快照 → M1~M6 全流程测试结果汇总")
        print("=" * 80)
        print(f"检查日期: {TEST_DATE}, 检查轮次: 1")
        print(f"快照记录数: {len(snapshot_records)}")
        plate_count = sum(1 for r in snapshot_records if r["mst_type"] != "PLATE_STOCKS")
        stock_count = sum(1 for r in snapshot_records if r["mst_type"] == "PLATE_STOCKS")
        print(f"  板块类型: {plate_count} 条, 成分股类型: {stock_count} 条")
        print("-" * 80)
        for r in all_results:
            detail = json.loads(r["detail"]) if r.get("detail") else {}
            extra_info = ""
            if r["dimension"] == Dimension.COMPLETENESS:
                extra_info = f"online={detail.get('online_count', '?')}, snapshot={detail.get('snapshot_count', '?')}"
            elif r["dimension"] == Dimension.TIMELINESS:
                extra_info = f"total_count={detail.get('total_count', '?')}"
            elif r["dimension"] == Dimension.ACCURACY:
                extra_info = f"total={detail.get('total', '?')}, errors={detail.get('errors', '?')}"
            print(f"  [{r['monitor_id']}] [{r['dimension']}] {r['result']}  {extra_info}")
        print("=" * 80)

    def test_full_pipeline_with_mismatch(
        self, mysql_storage, db_conn, snapshot_records, check_params, clean_all
    ):
        """完整流程：线上表少一条板块 → M1 FAIL，其他仍 PASS。"""
        # ── Step 1: 写入快照表 ──
        snapshot_repo = SnapshotRepository(mysql_storage)
        snapshot_repo.save(TEST_DATE, 1, snapshot_records)

        # ── Step 2: 线上表故意少一条板块（缺 0447.BK） ──
        insert_online_gmdb(db_conn, snapshot_records, exclude_stkcode="0447.BK")
        insert_online_ads_fin(db_conn, snapshot_records)

        # ── Step 3: M1 应 FAIL（缺板块） ──
        m1 = CompletenessChecker(
            monitor_id=MonitorID.M1, table=TABLE_GMDB,
            key_field="std_stkcode", mysql_storage=mysql_storage,
        )
        m1._pulsar_failed = False
        m1._pulsar_error = ""
        m1_result = m1._check(*check_params)
        assert m1_result["status"] == CheckResult.FAIL
        assert "0447.BK" in m1_result["missing"]
        m1._record(*check_params, m1_result)

        # ── Step 4: M4 应 PASS（ads_fin 完整） ──
        m4 = CompletenessChecker(
            monitor_id=MonitorID.M4, table=TABLE_ADS_FIN,
            key_field="compn_stock_code", mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        m4._pulsar_failed = False
        m4._pulsar_error = ""
        m4_result = m4._check(*check_params)
        assert m4_result["status"] == CheckResult.PASS
        m4._record(*check_params, m4_result)

        # ── Step 5: M2/M5 及时性应 PASS ──
        m2 = TimelinessChecker(monitor_id=MonitorID.M2, table=TABLE_GMDB, mysql_storage=mysql_storage)
        m2_result = m2._check(*check_params)
        assert m2_result["status"] == CheckResult.PASS
        m2._record(*check_params, m2_result)

        m5 = TimelinessChecker(monitor_id=MonitorID.M5, table=TABLE_ADS_FIN, mysql_storage=mysql_storage)
        m5_result = m5._check(*check_params)
        assert m5_result["status"] == CheckResult.PASS
        m5._record(*check_params, m5_result)

        # ── Step 6: M3/M6 准确性应 PASS ──
        m3 = AccuracyChecker(monitor_id=MonitorID.M3, table=TABLE_GMDB, key_field="stkcode", mysql_storage=mysql_storage)
        m3_result = m3._check(*check_params)
        assert m3_result["status"] == CheckResult.PASS
        m3._record(*check_params, m3_result)

        m6 = AccuracyChecker(monitor_id=MonitorID.M6, table=TABLE_ADS_FIN, key_field="stkcode", mysql_storage=mysql_storage)
        m6_result = m6._check(*check_params)
        assert m6_result["status"] == CheckResult.PASS
        m6._record(*check_params, m6_result)

        # ── Step 7: 验证结果持久化 ──
        result_repo = CheckResultRepository(mysql_storage)
        all_results = result_repo.get_by_date(TEST_DATE)
        assert len(all_results) == 6

        # M1 FAIL，其余 PASS
        m1_record = next(r for r in all_results if r["monitor_id"] == "M1")
        assert m1_record["result"] == "FAIL"
        for mid in ["M2", "M3", "M4", "M5", "M6"]:
            r = next(x for x in all_results if x["monitor_id"] == mid)
            assert r["result"] == "PASS", f"{mid} 应 PASS，实际: {r['result']}"
