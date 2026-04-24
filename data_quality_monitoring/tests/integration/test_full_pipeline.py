"""端到端集成测试：从 Pulsar topic 数据采集到数据质量检查全流程。

模拟场景：
1. 读取 testdate/ 中的 JSON 文件（模拟从 Pulsar topic 监听并解码后的数据）
2. 将解码后的数据写入快照表 dqm_security_info_snapshot（模拟 _prepare 阶段）
3. 向线上表插入与快照匹配的数据（模拟线上数据）
4. 对线上表执行完整性（M1/M4）、及时性（M2/M5）、准确性（M3/M6）检查
5. 验证检查结果正确写入 dqm_check_result

前置条件：
- 本地 MySQL 可连接，数据库 hidatapilot-dataclean 已存在
- 线上表 gmdb_plate_info、ads_fin_index_compn_stock_interface_ds 由测试自动创建和填充
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
# 测试文件位于: data_quality_monitoring/tests/integration/test_full_pipeline.py
# testdate 位于: testdate/ (项目根目录同级)
TESTDATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "testdate"

# 线上表名
TABLE_GMDB = "gmdb_plate_info"
TABLE_ADS_FIN = "ads_fin_index_compn_stock_interface_ds"

# 测试日期
TEST_DATE = date(2026, 4, 24)
TEST_SEND_DATE = 20260424


# ---------------------------------------------------------------------------
# 辅助函数：解码 topic 消息
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
    """将 Pulsar topic 解码后的原始消息转换为快照表记录。

    原始消息格式：
    - INDUSTRY_PLATE_INFO: {MSG_TYPE, SEND_DATE, SEND_TIME, CONTENT: [{stockCode, stockName}]}
    - PLATE_STOCKS: {MSG_TYPE, SEND_DATE, SEND_TIME, CONTENT: {item: {...}, stockList: [...]}}

    快照表字段：stkcode, stkname, std_stkcode, mst_type, compn_stock_code,
                compn_stock_name, index_name, send_date

    stkcode 格式转换：原始 "BK0477" → 存入 "0477.BK"，std_stkcode 存入 "0477.BK"
    """
    records = []

    for msg in messages:
        msg_type = msg.get("MSG_TYPE", "")
        send_date = msg.get("SEND_DATE", "")
        content = msg.get("CONTENT", {})

        if msg_type == "INDUSTRY_PLATE_INFO":
            # CONTENT 是板块列表 [{stockCode, stockName}, ...]
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
            # 同 INDUSTRY_PLATE_INFO 结构
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
            # CONTENT 是 {item: {plateCode, plateName, ...}, stockList: [{stockCode, stockName, ...}]}
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
    线上表 stkcode 取4位数字部分（满足 ^[0-9]{4}$ 正则），std_stkcode 与快照一致。

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
        # 线上表 stkcode: 取4位数字部分（与快照的数字部分一致，满足 ^[0-9]{4}$ 正则）
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
    """模块级别：确保 DQM 监控表存在。"""
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
def clean_all(db_conn):
    """每个测试前后：清理快照表 + 检查结果表 + 准确性明细表 + 线上表当天数据。"""
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.close()
    yield
    cur = db_conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.execute("DELETE FROM gmdb_plate_info WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.execute("DELETE FROM ads_fin_index_compn_stock_interface_ds WHERE send_date = %s", (TEST_SEND_DATE,))
    cur.close()


@pytest.fixture
def today() -> date:
    """测试日期：使用测试数据中的 SEND_DATE。"""
    return TEST_DATE


@pytest.fixture
def check_params(today):
    """通用检查参数。"""
    return today, datetime(2026, 4, 24, 9, 0, 0), 1


@pytest.fixture
def topic_messages():
    """加载模拟的 topic 消息。"""
    return load_testdata()


@pytest.fixture
def snapshot_records(topic_messages):
    """将 topic 消息解码为快照记录。"""
    return decode_topic_messages(topic_messages)


@pytest.fixture
def prepare_matched_data(db_conn, snapshot_records, clean_all):
    """准备"线上数据与 topic 数据完全一致"的场景。

    1. 将 topic 解码后的数据写入快照表
    2. 向线上表插入与快照匹配的数据
    """
    # 写入快照表
    with patch("src.dqm.storage.mysql_storage.MYSQL_HOST", TEST_DB_HOST), \
         patch("src.dqm.storage.mysql_storage.MYSQL_PORT", TEST_DB_PORT), \
         patch("src.dqm.storage.mysql_storage.MYSQL_USER", TEST_DB_USER), \
         patch("src.dqm.storage.mysql_storage.MYSQL_PASSWORD", TEST_DB_PASSWORD), \
         patch("src.dqm.storage.mysql_storage.MYSQL_DATABASE", TEST_DB_NAME), \
         patch("src.dqm.storage.mysql_storage.MYSQL_CHARSET", "utf8mb4"):
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


# ---------------------------------------------------------------------------
# 1. 数据采集流程测试：topic 消息解码 → 快照写入
# ---------------------------------------------------------------------------

class TestTopicDataCollection:
    """测试从 topic 消息解码到快照写入的全流程。"""

    def test_load_testdata_files(self, topic_messages):
        """应成功加载所有测试数据文件。"""
        assert len(topic_messages) >= 3, f"应至少加载 3 个测试数据文件，实际: {len(topic_messages)}"

    def test_decode_industry_plate_info(self, topic_messages):
        """应正确解码 INDUSTRY_PLATE_INFO 类型消息。"""
        records = decode_topic_messages(topic_messages)
        industry_records = [r for r in records if r["mst_type"] == "INDUSTRY_PLATE_INFO"]
        assert len(industry_records) > 0, "应有 INDUSTRY_PLATE_INFO 类型的记录"

        # 验证字段
        for r in industry_records:
            assert r["stkcode"], f"stkcode 不应为空: {r}"
            assert r["stkname"], f"stkname 不应为空: {r}"
            assert r["mst_type"] == "INDUSTRY_PLATE_INFO"

        # 验证已知的板块代码（4位左补零格式）
        stkcodes = {r["stkcode"] for r in industry_records}
        assert "0477.BK" in stkcodes, f"应包含酿酒行业板块 0477.BK，实际: {stkcodes}"

    def test_decode_plate_stocks(self, topic_messages):
        """应正确解码 PLATE_STOCKS 类型消息。"""
        records = decode_topic_messages(topic_messages)
        plate_stocks = [r for r in records if r["mst_type"] == "PLATE_STOCKS"]
        assert len(plate_stocks) > 0, "应有 PLATE_STOCKS 类型的记录"

        # 验证字段
        for r in plate_stocks:
            assert r["stkcode"], f"stkcode(plateCode) 不应为空: {r}"
            assert r["compn_stock_code"], f"compn_stock_code 不应为空: {r}"
            assert r["compn_stock_name"], f"compn_stock_name 不应为空: {r}"
            assert r["mst_type"] == "PLATE_STOCKS"

        # 验证已知的成分股代码
        stock_codes = {r["compn_stock_code"] for r in plate_stocks}
        assert "SH600519" in stock_codes, f"应包含贵州茅台 SH600519，实际: {stock_codes}"
        assert "SH600000" in stock_codes, f"应包含浦发银行 SH600000，实际: {stock_codes}"

    def test_snapshot_write_and_read(self, mysql_storage, snapshot_records, today, clean_all):
        """应成功将快照记录写入 MySQL 并能读回。"""
        snapshot_repo = SnapshotRepository(mysql_storage)

        # 写入
        snapshot_repo.save(today, 1, snapshot_records)

        # 读回
        saved = snapshot_repo.get_by_date(today)
        assert len(saved) == len(snapshot_records), (
            f"写入和读回数量不一致: written={len(snapshot_records)}, read={len(saved)}"
        )

    def test_snapshot_idempotent(self, mysql_storage, snapshot_records, today, clean_all):
        """同一天同一轮的快照数据 INSERT IGNORE 应幂等。"""
        snapshot_repo = SnapshotRepository(mysql_storage)

        # 写入两次
        snapshot_repo.save(today, 1, snapshot_records)
        snapshot_repo.save(today, 1, snapshot_records)

        # 读回，数量不变
        saved = snapshot_repo.get_by_date(today)
        assert len(saved) == len(snapshot_records)


# ---------------------------------------------------------------------------
# 2. M1 完整性检查端到端：topic 数据 → 快照 → 比对 gmdb_plate_info
# ---------------------------------------------------------------------------

class TestM1CompletenessPipeline:
    """M1 完整性检查全流程：快照 vs gmdb_plate_info.std_stkcode。"""

    def test_m1_check_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M1: 线上表与快照完全一致 → PASS。"""
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
        assert result["missing"] == []
        assert result["extra"] == []

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m1_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M1: 通过 execute() 端到端执行（mock _prepare 跳过 Pulsar 采集）。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table=TABLE_GMDB,
            key_field="std_stkcode",
            mysql_storage=mysql_storage,
        )

        # mock _prepare：跳过 Pulsar 采集，直接标记成功
        def mock_prepare(check_date, check_time, check_round):
            checker._pulsar_failed = False
            checker._pulsar_error = ""

        with patch.object(checker, "_prepare", side_effect=mock_prepare):
            checker.execute(*check_params)

        # 验证结果已写入
        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M1)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.COMPLETENESS
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 3. M4 完整性检查端到端：topic 数据 → 快照 → 比对 ads_fin_index
# ---------------------------------------------------------------------------

class TestM4CompletenessPipeline:
    """M4 完整性检查全流程：快照 vs ads_fin_index_compn_stock_interface_ds.compn_stock_code。"""

    def test_m4_check_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M4: 线上表与快照完全一致 → PASS。"""
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
        assert result["group_details"] == []

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m4_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M4: 通过 execute() 端到端执行。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M4,
            table=TABLE_ADS_FIN,
            key_field="compn_stock_code",
            mysql_storage=mysql_storage,
            group_field="stkcode",
        )

        def mock_prepare(check_date, check_time, check_round):
            checker._pulsar_failed = False
            checker._pulsar_error = ""

        with patch.object(checker, "_prepare", side_effect=mock_prepare):
            checker.execute(*check_params)

        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M4)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.COMPLETENESS
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 4. M2 及时性检查端到端：线上表 gmdb_plate_info 当天数据
# ---------------------------------------------------------------------------

class TestM2TimelinessPipeline:
    """M2 及时性检查：gmdb_plate_info 是否有当天数据。"""

    def test_m2_timeliness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M2: gmdb_plate_info 有当天数据 → PASS。"""
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

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m2_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M2: 通过 execute() 端到端执行。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M2,
            table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )

        checker.execute(*check_params)

        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M2)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.TIMELINESS
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 5. M5 及时性检查端到端：线上表 ads_fin_index 当天数据
# ---------------------------------------------------------------------------

class TestM5TimelinessPipeline:
    """M5 及时性检查：ads_fin_index_compn_stock_interface_ds 是否有当天数据。"""

    def test_m5_timeliness_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M5: ads_fin_index 有当天数据 → PASS。"""
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

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m5_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M5: 通过 execute() 端到端执行。"""
        checker = TimelinessChecker(
            monitor_id=MonitorID.M5,
            table=TABLE_ADS_FIN,
            mysql_storage=mysql_storage,
        )

        checker.execute(*check_params)

        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M5)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.TIMELINESS
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 6. M3 准确性检查端到端：gmdb_plate_info 字段校验
# ---------------------------------------------------------------------------

class TestM3AccuracyPipeline:
    """M3 准确性检查：gmdb_plate_info 字段非空和类型约束。"""

    def test_m3_accuracy_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M3: gmdb_plate_info 字段均合法 → PASS。"""
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

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m3_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M3: 通过 execute() 端到端执行。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M3,
            table=TABLE_GMDB,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )

        checker.execute(*check_params)

        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M3)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.ACCURACY
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 7. M6 准确性检查端到端：ads_fin_index 字段校验
# ---------------------------------------------------------------------------

class TestM6AccuracyPipeline:
    """M6 准确性检查：ads_fin_index_compn_stock_interface_ds 字段非空和类型约束。"""

    def test_m6_accuracy_pass(self, mysql_storage, prepare_matched_data, check_params):
        """M6: ads_fin_index 字段均合法 → PASS。"""
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

        # 记录并验证
        checker._record(*check_params, result)
        checker._alert(*check_params, result)

    def test_m6_execute_end_to_end(self, mysql_storage, prepare_matched_data, today, check_params):
        """M6: 通过 execute() 端到端执行。"""
        checker = AccuracyChecker(
            monitor_id=MonitorID.M6,
            table=TABLE_ADS_FIN,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )

        checker.execute(*check_params)

        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M6)
        assert len(records) == 1
        assert records[0]["dimension"] == Dimension.ACCURACY
        assert records[0]["result"] == "PASS"


# ---------------------------------------------------------------------------
# 8. 全链路集成测试：一次完整的 M1~M6 检查流程
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """模拟一次完整的数据质量监控流程：
    topic 数据 → 快照写入 → M1~M6 全部执行 → 结果验证

    这是核心的端到端测试，验证从 topic 数据采集到所有监控项检查的完整流程。
    """

    def test_full_pipeline_all_monitors(
        self, mysql_storage, db_conn, snapshot_records, today, check_params, clean_all
    ):
        """完整流程：topic 解码 → 快照写入 → M1/M2/M3/M4/M5/M6 全部检查。"""
        # ── Step 1: 模拟 topic 数据写入快照表 ──
        snapshot_repo = SnapshotRepository(mysql_storage)
        snapshot_repo.save(today, 1, snapshot_records)

        # 验证快照写入成功
        saved = snapshot_repo.get_by_date(today)
        assert len(saved) > 0, "快照表应有数据"

        # 分类统计快照数据
        industry_count = sum(1 for r in saved if r.get("mst_type") == "INDUSTRY_PLATE_INFO")
        plate_stocks_count = sum(1 for r in saved if r.get("mst_type") == "PLATE_STOCKS")
        assert industry_count > 0, "快照中应有 INDUSTRY_PLATE_INFO 数据"
        assert plate_stocks_count > 0, "快照中应有 PLATE_STOCKS 数据"

        # ── Step 2: 向线上表插入匹配数据 ──
        insert_online_gmdb(db_conn, snapshot_records)
        insert_online_ads_fin(db_conn, snapshot_records)

        # ── Step 3: M1 完整性检查 ──
        m1 = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table=TABLE_GMDB,
            key_field="std_stkcode",
            mysql_storage=mysql_storage,
        )
        m1._pulsar_failed = False
        m1._pulsar_error = ""
        m1_result = m1._check(*check_params)

        assert m1_result["status"] == CheckResult.PASS, (
            f"M1 应 PASS | online={m1_result['online_count']}, snapshot={m1_result['snapshot_count']}, "
            f"missing={m1_result['missing']}, extra={m1_result['extra']}"
        )
        m1._record(*check_params, m1_result)
        m1._alert(*check_params, m1_result)

        # ── Step 4: M4 完整性检查 ──
        m4 = CompletenessChecker(
            monitor_id=MonitorID.M4,
            table=TABLE_ADS_FIN,
            key_field="compn_stock_code",
            mysql_storage=mysql_storage,
            group_field="stkcode",
        )
        m4._pulsar_failed = False
        m4._pulsar_error = ""
        m4_result = m4._check(*check_params)

        assert m4_result["status"] == CheckResult.PASS, (
            f"M4 应 PASS | online={m4_result['online_count']}, snapshot={m4_result['snapshot_count']}, "
            f"group_details={m4_result['group_details']}"
        )
        m4._record(*check_params, m4_result)
        m4._alert(*check_params, m4_result)

        # ── Step 5: M2 及时性检查 ──
        m2 = TimelinessChecker(
            monitor_id=MonitorID.M2,
            table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )
        m2_result = m2._check(*check_params)

        assert m2_result["status"] == CheckResult.PASS, f"M2 应 PASS | total_count={m2_result['total_count']}"
        m2._record(*check_params, m2_result)
        m2._alert(*check_params, m2_result)

        # ── Step 6: M5 及时性检查 ──
        m5 = TimelinessChecker(
            monitor_id=MonitorID.M5,
            table=TABLE_ADS_FIN,
            mysql_storage=mysql_storage,
        )
        m5_result = m5._check(*check_params)

        assert m5_result["status"] == CheckResult.PASS, f"M5 应 PASS | total_count={m5_result['total_count']}"
        m5._record(*check_params, m5_result)
        m5._alert(*check_params, m5_result)

        # ── Step 7: M3 准确性检查 ──
        m3 = AccuracyChecker(
            monitor_id=MonitorID.M3,
            table=TABLE_GMDB,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )
        m3_result = m3._check(*check_params)

        assert m3_result["status"] == CheckResult.PASS, (
            f"M3 应 PASS | total={m3_result['total']}, errors={m3_result['errors']}, "
            f"details={m3_result['details']}"
        )
        m3._record(*check_params, m3_result)
        m3._alert(*check_params, m3_result)

        # ── Step 8: M6 准确性检查 ──
        m6 = AccuracyChecker(
            monitor_id=MonitorID.M6,
            table=TABLE_ADS_FIN,
            key_field="stkcode",
            mysql_storage=mysql_storage,
        )
        m6_result = m6._check(*check_params)

        assert m6_result["status"] == CheckResult.PASS, (
            f"M6 应 PASS | total={m6_result['total']}, errors={m6_result['errors']}, "
            f"details={m6_result['details']}"
        )
        m6._record(*check_params, m6_result)
        m6._alert(*check_params, m6_result)

        # ── Step 9: 验证所有检查结果已写入 dqm_check_result ──
        result_repo = CheckResultRepository(mysql_storage)
        all_results = result_repo.get_by_date(today)
        assert len(all_results) == 6, f"应有 6 条检查结果，实际: {len(all_results)}"

        # 验证各监控项均有结果
        monitor_ids = {r["monitor_id"] for r in all_results}
        assert monitor_ids == {"M1", "M2", "M3", "M4", "M5", "M6"}

        # 验证各维度均有结果
        dimensions = {r["dimension"] for r in all_results}
        assert Dimension.COMPLETENESS in dimensions
        assert Dimension.TIMELINESS in dimensions
        assert Dimension.ACCURACY in dimensions

        # 验证全部 PASS
        for r in all_results:
            assert r["result"] == "PASS", f"[{r['monitor_id']}] 应为 PASS，实际: {r['result']}"

        # ── Step 10: 输出汇总 ──
        print("\n" + "=" * 80)
        print("数据质量监控端到端测试结果汇总")
        print("=" * 80)
        print(f"检查日期: {today}, 检查轮次: 1")
        print(f"快照数据: INDUSTRY_PLATE_INFO={industry_count}, PLATE_STOCKS={plate_stocks_count}")
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

    def test_pipeline_idempotency(
        self, mysql_storage, db_conn, snapshot_records, today, check_params, clean_all
    ):
        """幂等性测试：同一轮检查重复执行不产生重复结果。"""
        # 写入快照
        snapshot_repo = SnapshotRepository(mysql_storage)
        snapshot_repo.save(today, 1, snapshot_records)

        # 插入线上数据
        insert_online_gmdb(db_conn, snapshot_records)

        m2 = TimelinessChecker(
            monitor_id=MonitorID.M2,
            table=TABLE_GMDB,
            mysql_storage=mysql_storage,
        )

        # 执行两次
        m2_result = m2._check(*check_params)
        m2._record(*check_params, m2_result)

        m2_result2 = m2._check(*check_params)
        m2._record(*check_params, m2_result2)

        # 验证只有一条记录（upsert 幂等）
        result_repo = CheckResultRepository(mysql_storage)
        records = result_repo.get_by_date(today, MonitorID.M2)
        assert len(records) == 1, f"幂等写入后应只有 1 条记录，实际: {len(records)}"
