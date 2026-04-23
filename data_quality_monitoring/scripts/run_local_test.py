#!/usr/bin/env python
"""本地端到端测试脚本：造模拟数据 + 连接本地数据库 + 执行全量 M1-M6 检查。

无需 Pulsar/MongoDB，仅依赖本地 MySQL。
模拟数据包括：
  1. 业务表 gmdb_plate_info / ads_fin_index_compn_stock_interface_ds
  2. DQM 系统表 dqm_security_info_snapshot / dqm_check_result / dqm_accuracy_detail
  3. 快照表模拟 Pulsar 采集结果（M1/M4 共享）

使用方式：
  cd data_quality_monitoring
  python scripts/run_local_test.py
"""

import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

# 将项目根目录加入 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pymysql
from pymysql.cursors import DictCursor

from config.constants import CheckResult, Dimension, MonitorID
from config.logger_config import setup_logger, get_logger
from src.dqm.checkers.accuracy import AccuracyChecker
from src.dqm.checkers.completeness import CompletenessChecker
from src.dqm.checkers.timeliness import TimelinessChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.schema import ALL_DDL

# ── 测试数据库配置 ──────────────────────────────────────
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 3306
TEST_DB_USER = "root"
TEST_DB_PASSWORD = "hh"
TEST_DB_NAME = "hidatapilot-dataclean"

# 检查日期（使用一个固定日期来保证数据一致性）
CHECK_DATE = date(2026, 4, 23)
CHECK_TIME = datetime(2026, 4, 23, 9, 0, 0)
SEND_DATE_INT = 20260423  # YYYYMMDD 整数格式


# ═══════════════════════════════════════════════════════════
# 1. 业务表 DDL + 模拟数据
# ═══════════════════════════════════════════════════════════

DDL_GMDB_PLATE_INFO = """
CREATE TABLE IF NOT EXISTS `gmdb_plate_info` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `stkcode` VARCHAR(20) NOT NULL COMMENT '板块代码',
    `stkname` VARCHAR(100) NOT NULL COMMENT '板块名称',
    `std_stkcode` VARCHAR(30) NOT NULL COMMENT '标准板块代码',
    `zhishubankuaileibie` VARCHAR(50) NOT NULL COMMENT '指数板块类别',
    `mst_type` VARCHAR(50) NOT NULL COMMENT '消息类型',
    `send_date` INT NOT NULL COMMENT '发送日期 YYYYMMDD',
    `send_time` INT DEFAULT NULL COMMENT '发送时间 HHMMSS',
    `index_type_name` VARCHAR(100) DEFAULT NULL COMMENT '指数类型名称',
    INDEX `idx_send_date` (`send_date`),
    INDEX `idx_mst_type` (`mst_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块信息表';
"""

DDL_ADS_FIN_INDEX = """
CREATE TABLE IF NOT EXISTS `ads_fin_index_compn_stock_interface_ds` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `stkcode` VARCHAR(20) NOT NULL COMMENT '板块代码',
    `compn_stock_code` VARCHAR(20) NOT NULL COMMENT '成分股代码',
    `compn_stock_name` VARCHAR(100) NOT NULL COMMENT '成分股名称',
    `index_name` VARCHAR(100) NOT NULL COMMENT '指数名称',
    `send_date` INT NOT NULL COMMENT '发送日期 YYYYMMDD',
    `compn_stock_thscode` VARCHAR(30) DEFAULT NULL COMMENT '同花顺代码',
    `valid_from` INT DEFAULT NULL COMMENT '生效开始日期',
    `valid_to` INT DEFAULT NULL COMMENT '生效结束日期',
    `timestamp` BIGINT DEFAULT NULL COMMENT '时间戳',
    INDEX `idx_send_date` (`send_date`),
    INDEX `idx_stkcode` (`stkcode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数成分股接口表';
"""

# ── gmdb_plate_info 模拟数据 ──
# M1 完整性：需要 INDUSTRY_PLATE_INFO / REGION_PLATE_INFO / HOTIDEA_PLATE_INFO 三种类型的 stkcode
# M2 及时性：需要 send_date = 20260423 的数据
# M3 准确性：需要符合字段约束的数据（部分故意设错来验证 FAIL 场景）
MOCK_GMDB_PLATE_INFO = [
    # ── INDUSTRY_PLATE_INFO（行业板块） ──
    {"stkcode": "0424", "stkname": "有色金属", "std_stkcode": "042400.BK", "zhishubankuaileibie": "行业板块",
     "mst_type": "INDUSTRY_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90000, "index_type_name": None},
    {"stkcode": "0438", "stkname": "医药制造", "std_stkcode": "043800.BK", "zhishubankuaileibie": "行业板块",
     "mst_type": "INDUSTRY_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90001, "index_type_name": None},
    {"stkcode": "0439", "stkname": "电子元件", "std_stkcode": "043900.BK", "zhishubankuaileibie": "行业板块",
     "mst_type": "INDUSTRY_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90002, "index_type_name": None},
    # ── REGION_PLATE_INFO（地区板块） ──
    {"stkcode": "0443", "stkname": "北京板块", "std_stkcode": "044300.BK", "zhishubankuaileibie": "地区板块",
     "mst_type": "REGION_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90003, "index_type_name": None},
    {"stkcode": "0447", "stkname": "上海板块", "std_stkcode": "044700.BK", "zhishubankuaileibie": "地区板块",
     "mst_type": "REGION_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90004, "index_type_name": None},
    # ── HOTIDEA_PLATE_INFO（热门概念板块） ──
    {"stkcode": "0477", "stkname": "酿酒行业", "std_stkcode": "047700.BK", "zhishubankuaileibie": "概念板块",
     "mst_type": "HOTIDEA_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90005, "index_type_name": None},
    # ── PLATE_STOCKS（板块成分股 — M4 用） ──
    {"stkcode": "0001.BK", "stkname": "上证50板块", "std_stkcode": "000100.BK", "zhishubankuaileibie": "指数板块",
     "mst_type": "PLATE_STOCKS", "send_date": SEND_DATE_INT, "send_time": 90010, "index_type_name": "上证50"},
    # PLATE_STOCKS 类型在快照中也有 0443.BK / 0477.BK 的 stkcode，
    # 线上表也需要有对应记录，否则 M1 会报 missing
    {"stkcode": "0443.BK", "stkname": "沪深300板块", "std_stkcode": "044300.BK", "zhishubankuaileibie": "指数板块",
     "mst_type": "PLATE_STOCKS", "send_date": SEND_DATE_INT, "send_time": 90011, "index_type_name": "沪深300"},
    {"stkcode": "0477.BK", "stkname": "深证成指板块", "std_stkcode": "047700.BK", "zhishubankuaileibie": "指数板块",
     "mst_type": "PLATE_STOCKS", "send_date": SEND_DATE_INT, "send_time": 90012, "index_type_name": "深证成指"},

    # ── M3 准确性：故意造一条有问题的数据 ──
    # stkcode 不匹配 ^[0-9]{6}$ → REGEX_MISMATCH
    # mst_type 不在枚举中 → ENUM_INVALID
    {"stkcode": "BAD1", "stkname": "异常数据", "std_stkcode": "bad_code", "zhishubankuaileibie": "测试板块",
     "mst_type": "INVALID_TYPE", "send_date": SEND_DATE_INT, "send_time": None, "index_type_name": None},
    # send_date 不是数字 → TYPE_MISMATCH (但它是 int 不好模拟，用正常值即可)
    # std_stkcode 不匹配 ^[0-9]{6}\.[A-Z]{2}$ → REGEX_MISMATCH
    {"stkcode": "049999", "stkname": "格式错误", "std_stkcode": "wrong-format", "zhishubankuaileibie": "行业板块",
     "mst_type": "INDUSTRY_PLATE_INFO", "send_date": SEND_DATE_INT, "send_time": 90006, "index_type_name": None},

    # ── 历史日期数据（不应被当天检查命中） ──
    {"stkcode": "0424", "stkname": "有色金属", "std_stkcode": "042400.BK", "zhishubankuaileibie": "行业板块",
     "mst_type": "INDUSTRY_PLATE_INFO", "send_date": 20260422, "send_time": 90000, "index_type_name": None},
]

# ── ads_fin_index_compn_stock_interface_ds 模拟数据 ──
# M4 完整性：按 stkcode 分组比对 compn_stock_code
# M5 及时性：需要 send_date = 20260423 的数据
# M6 准确性：需要符合字段约束的数据（部分故意设错）
MOCK_ADS_FIN_INDEX = [
    # 上证50 (stkcode=0001.BK) 的成分股
    {"stkcode": "0001.BK", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
     "index_name": "上证50", "send_date": SEND_DATE_INT, "compn_stock_thscode": "600000",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830400},
    {"stkcode": "0001.BK", "compn_stock_code": "SH600001", "compn_stock_name": "邯郸钢铁",
     "index_name": "上证50", "send_date": SEND_DATE_INT, "compn_stock_thscode": "600001",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830401},
    {"stkcode": "0001.BK", "compn_stock_code": "SH600004", "compn_stock_name": "白云机场",
     "index_name": "上证50", "send_date": SEND_DATE_INT, "compn_stock_thscode": "600004",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830402},

    # 深证成指 (stkcode=0477.BK) 的成分股
    {"stkcode": "0477.BK", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行",
     "index_name": "深证成指", "send_date": SEND_DATE_INT, "compn_stock_thscode": "000001",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830403},
    {"stkcode": "0477.BK", "compn_stock_code": "SZ000002", "compn_stock_name": "万科A",
     "index_name": "深证成指", "send_date": SEND_DATE_INT, "compn_stock_thscode": "000002",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830404},

    # 沪深300 (stkcode=0443.BK) 的成分股
    {"stkcode": "0443.BK", "compn_stock_code": "SH601318", "compn_stock_name": "中国平安",
     "index_name": "沪深300", "send_date": SEND_DATE_INT, "compn_stock_thscode": "601318",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830405},

    # ── M6 准确性：故意造一条有问题的数据 ──
    # compn_stock_name 为空 → NULL_VALUE (required=True)
    {"stkcode": "0438.BK", "compn_stock_code": "SZ300001", "compn_stock_name": "",
     "index_name": "创业板指", "send_date": SEND_DATE_INT, "compn_stock_thscode": "300001",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830406},
    # index_name 为空 → NULL_VALUE (required=True)
    {"stkcode": "0439.BK", "compn_stock_code": "SZ300002", "compn_stock_name": "神州泰岳",
     "index_name": "", "send_date": SEND_DATE_INT, "compn_stock_thscode": "300002",
     "valid_from": 20200101, "valid_to": 99991231, "timestamp": 1713830407},
]


# ═══════════════════════════════════════════════════════════
# 2. 快照模拟数据（替代 Pulsar 采集）
# ═══════════════════════════════════════════════════════════

# M1 需要的快照（mst_type 为 INDUSTRY/REGION/HOTIDEA 的 stkcode）
# M4 需要的快照（mst_type 为 PLATE_STOCKS 的 stkcode + compn_stock_code）
# 两者共享同一张 dqm_security_info_snapshot 表
#
# 关键：M1 的 _query_snapshot_keys 查所有 check_date 的 DISTINCT stkcode（不限 mst_type），
# 而 M1 的 _query_online_keys 查 gmdb_plate_info 的 DISTINCT stkcode（不限 mst_type）。
# 所以快照中 PLATE_STOCKS 类型的 stkcode 也会被 M1 比对到。
# 如果快照中 stkcode 与线上表不完全对应，就会出现 missing/extra。
MOCK_SNAPSHOT = [
    # ── M1: 行业/地区/概念板块（与线上表 INDUSTRY/REGION/HOTIDEA 一致） ──
    {"stkcode": "0424", "stkname": "有色金属", "std_stkcode": "042400.BK",
     "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0438", "stkname": "医药制造", "std_stkcode": "043800.BK",
     "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0439", "stkname": "电子元件", "std_stkcode": "043900.BK",
     "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0443", "stkname": "北京板块", "std_stkcode": "044300.BK",
     "mst_type": "REGION_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0447", "stkname": "上海板块", "std_stkcode": "044700.BK",
     "mst_type": "REGION_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0477", "stkname": "酿酒行业", "std_stkcode": "047700.BK",
     "mst_type": "HOTIDEA_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    # 快照中包含了线上表的 PLATE_STOCKS 类型 (0001.BK) 和异常数据 (BAD1, 049999)，
    # 这样 M1 的线上 stkcode 集合和快照 stkcode 集合就完全一致 → PASS
    {"stkcode": "0001.BK", "stkname": "上证50板块", "std_stkcode": "000100.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "BAD1", "stkname": "异常数据", "std_stkcode": "bad_code",
     "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "049999", "stkname": "格式错误", "std_stkcode": "wrong-format",
     "mst_type": "INDUSTRY_PLATE_INFO", "compn_stock_code": "", "compn_stock_name": "",
     "index_name": "", "send_date": str(SEND_DATE_INT)},

    # ── M4: 板块成分股（PLATE_STOCKS 类型，带 compn_stock_code） ──
    # 注意：唯一键是 (check_date, stkcode, mst_type, compn_stock_code)，
    # 所以上面 compn_stock_code="" 的 PLATE_STOCKS 记录和下面的不会冲突。
    {"stkcode": "0001.BK", "stkname": "上证50板块", "std_stkcode": "000100.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SH600000", "compn_stock_name": "浦发银行",
     "index_name": "上证50", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0001.BK", "stkname": "上证50板块", "std_stkcode": "000100.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SH600001", "compn_stock_name": "邯郸钢铁",
     "index_name": "上证50", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0001.BK", "stkname": "上证50板块", "std_stkcode": "000100.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SH600004", "compn_stock_name": "白云机场",
     "index_name": "上证50", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0477.BK", "stkname": "酿酒行业", "std_stkcode": "047700.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SZ000001", "compn_stock_name": "平安银行",
     "index_name": "深证成指", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0477.BK", "stkname": "酿酒行业", "std_stkcode": "047700.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SZ000002", "compn_stock_name": "万科A",
     "index_name": "深证成指", "send_date": str(SEND_DATE_INT)},
    {"stkcode": "0443.BK", "stkname": "北京板块", "std_stkcode": "044300.BK",
     "mst_type": "PLATE_STOCKS", "compn_stock_code": "SH601318", "compn_stock_name": "中国平安",
     "index_name": "沪深300", "send_date": str(SEND_DATE_INT)},
    # M4 快照中故意没有 0438.BK / 0439.BK / 0447.BK 的 PLATE_STOCKS 数据
    # 线上表 0438.BK 有 SZ300001，0439.BK 有 SZ300002，但快照没有 → extra
    # 线上表 0447.BK 无数据，快照也无数据 → 该组不出现在差异中
]


# ═══════════════════════════════════════════════════════════
# 3. 辅助函数
# ═══════════════════════════════════════════════════════════

def get_raw_connection():
    """获取原始 pymysql 连接（用于 DDL 和批量数据插入）。"""
    return pymysql.connect(
        host=TEST_DB_HOST, port=TEST_DB_PORT,
        user=TEST_DB_USER, password=TEST_DB_PASSWORD,
        database=TEST_DB_NAME, charset="utf8mb4", autocommit=True,
    )


# 全局 patch 管理器（在整个测试期间保持 patch 生效）
_patches = []


def start_patching():
    """启动全局 patch（覆盖 MySQLStorage 中的模块级常量）。"""
    targets = {
        "src.dqm.storage.mysql_storage.MYSQL_HOST": TEST_DB_HOST,
        "src.dqm.storage.mysql_storage.MYSQL_PORT": TEST_DB_PORT,
        "src.dqm.storage.mysql_storage.MYSQL_USER": TEST_DB_USER,
        "src.dqm.storage.mysql_storage.MYSQL_PASSWORD": TEST_DB_PASSWORD,
        "src.dqm.storage.mysql_storage.MYSQL_DATABASE": TEST_DB_NAME,
        "src.dqm.storage.mysql_storage.MYSQL_CHARSET": "utf8mb4",
    }
    for target, value in targets.items():
        p = patch(target, value)
        p.start()
        _patches.append(p)


def stop_patching():
    """停止全局 patch。"""
    for p in _patches:
        p.stop()
    _patches.clear()


def get_mysql_storage():
    """创建 MySQLStorage 实例（需在 start_patching 后调用）。"""
    return MySQLStorage()


def setup_tables(conn):
    """创建所有必要的表（业务表 + DQM 系统表）。"""
    cur = conn.cursor()

    # 先删除旧表（注意外键顺序）
    cur.execute("DROP TABLE IF EXISTS `dqm_accuracy_detail`")
    cur.execute("DROP TABLE IF EXISTS `dqm_check_result`")
    cur.execute("DROP TABLE IF EXISTS `dqm_security_info_snapshot`")
    cur.execute("DROP TABLE IF EXISTS `ads_fin_index_compn_stock_interface_ds`")
    cur.execute("DROP TABLE IF EXISTS `gmdb_plate_info`")

    # 创建业务表
    cur.execute(DDL_GMDB_PLATE_INFO)
    cur.execute(DDL_ADS_FIN_INDEX)

    # 创建 DQM 系统表
    for ddl in ALL_DDL:
        cur.execute(ddl)

    cur.close()
    print("  [OK] 所有表创建完成")


def insert_business_data(conn):
    """插入业务表模拟数据。"""
    cur = conn.cursor()

    # gmdb_plate_info
    sql_gmdb = """
    INSERT INTO gmdb_plate_info
    (stkcode, stkname, std_stkcode, zhishubankuaileibie, mst_type, send_date, send_time, index_type_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    params_gmdb = [
        (r["stkcode"], r["stkname"], r["std_stkcode"], r["zhishubankuaileibie"],
         r["mst_type"], r["send_date"], r["send_time"], r["index_type_name"])
        for r in MOCK_GMDB_PLATE_INFO
    ]
    cur.executemany(sql_gmdb, params_gmdb)
    print(f"  [OK] gmdb_plate_info 插入 {len(params_gmdb)} 条")

    # ads_fin_index_compn_stock_interface_ds
    sql_ads = """
    INSERT INTO ads_fin_index_compn_stock_interface_ds
    (stkcode, compn_stock_code, compn_stock_name, index_name, send_date,
     compn_stock_thscode, valid_from, valid_to, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params_ads = [
        (r["stkcode"], r["compn_stock_code"], r["compn_stock_name"], r["index_name"],
         r["send_date"], r["compn_stock_thscode"], r["valid_from"], r["valid_to"], r["timestamp"])
        for r in MOCK_ADS_FIN_INDEX
    ]
    cur.executemany(sql_ads, params_ads)
    print(f"  [OK] ads_fin_index_compn_stock_interface_ds 插入 {len(params_ads)} 条")

    cur.close()


def insert_snapshot_data(conn, check_round: int = 1):
    """插入快照模拟数据（替代 Pulsar 采集）。"""
    cur = conn.cursor()
    sql = """
    INSERT IGNORE INTO dqm_security_info_snapshot
    (check_date, check_round, check_time, stkcode, stkname, std_stkcode, mst_type,
     compn_stock_code, compn_stock_name, index_name, send_date)
    VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (CHECK_DATE, check_round,
         r["stkcode"], r["stkname"], r["std_stkcode"], r["mst_type"],
         r["compn_stock_code"], r["compn_stock_name"], r["index_name"], r["send_date"])
        for r in MOCK_SNAPSHOT
    ]
    cur.executemany(sql, params)
    print(f"  [OK] dqm_security_info_snapshot 插入 {len(params)} 条")
    cur.close()


def clear_dqm_tables(conn):
    """清理 DQM 系统表数据。"""
    cur = conn.cursor()
    cur.execute("DELETE FROM dqm_accuracy_detail")
    cur.execute("DELETE FROM dqm_check_result")
    cur.execute("DELETE FROM dqm_security_info_snapshot")
    cur.close()


def query_check_result(conn, monitor_id: str) -> dict | None:
    """查询检查结果。"""
    cur = conn.cursor(DictCursor)
    cur.execute(
        "SELECT * FROM dqm_check_result WHERE check_date = %s AND monitor_id = %s",
        (CHECK_DATE, monitor_id),
    )
    row = cur.fetchone()
    cur.close()
    return row


def query_accuracy_details(conn, monitor_id: str) -> list[dict]:
    """查询准确性明细。"""
    cur = conn.cursor(DictCursor)
    cur.execute(
        "SELECT * FROM dqm_accuracy_detail WHERE check_date = %s AND monitor_id = %s",
        (CHECK_DATE, monitor_id),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


# ═══════════════════════════════════════════════════════════
# 4. 各监控项检查 + 验证
# ═══════════════════════════════════════════════════════════

def run_m1(storage: MySQLStorage, conn) -> dict:
    """M1: gmdb_plate_info 完整性检查。"""
    print("\n" + "=" * 60)
    print("M1: gmdb_plate_info 完整性检查")
    print("=" * 60)

    checker = CompletenessChecker(
        monitor_id=MonitorID.M1,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=storage,
    )

    # 标记 Pulsar 未失败，跳过 _prepare（快照已手动插入）
    checker._pulsar_failed = False
    checker._pulsar_error = ""

    # 直接调用 _check → _record → _alert
    result = checker._check(CHECK_DATE, CHECK_TIME, 1)
    checker._record(CHECK_DATE, CHECK_TIME, 1, result)
    checker._alert(CHECK_DATE, CHECK_TIME, 1, result)

    # 验证
    db_result = query_check_result(conn, "M1")
    print(f"\n  状态: {result['status']}")
    print(f"  线上表 stkcode 数: {result['online_count']}")
    print(f"  快照表 stkcode 数: {result['snapshot_count']}")
    print(f"  missing (快照有线上无): {result['missing']}")
    print(f"  extra (线上有快照无): {result['extra']}")
    print(f"  DB 结果: monitor_id={db_result['monitor_id']}, result={db_result['result'] if db_result else 'N/A'}")

    # 预期：快照中包含了线上表的所有 stkcode（含 PLATE_STOCKS 的 0001.BK、异常数据 BAD1/049999），
    # 线上表的 stkcode 集合与快照完全一致 → PASS
    assert db_result is not None, "M1 结果应已写入数据库"
    assert db_result["dimension"] == "完整性"
    assert result["status"] == CheckResult.PASS, f"M1 完整性应为 PASS, 实际: {result['status']}"
    assert result["missing"] == [], f"M1 missing 应为空, 实际: {result['missing']}"
    assert result["extra"] == [], f"M1 extra 应为空, 实际: {result['extra']}"
    print("  [PASS] M1 检查完成")

    return {"monitor": "M1", "result": result, "db": db_result}


def run_m2(storage: MySQLStorage, conn) -> dict:
    """M2: gmdb_plate_info 及时性检查。"""
    print("\n" + "=" * 60)
    print("M2: gmdb_plate_info 及时性检查")
    print("=" * 60)

    checker = TimelinessChecker(
        monitor_id=MonitorID.M2,
        table="gmdb_plate_info",
        mysql_storage=storage,
    )

    # M2 不需要 Pulsar，直接执行
    checker.execute(CHECK_DATE, CHECK_TIME, 1)

    # 验证
    db_result = query_check_result(conn, "M2")
    print(f"\n  DB 结果: monitor_id={db_result['monitor_id'] if db_result else 'N/A'}, "
          f"result={db_result['result'] if db_result else 'N/A'}")

    assert db_result is not None, "M2 结果应已写入数据库"
    assert db_result["result"] == CheckResult.PASS, f"M2 应为 PASS (当天有数据), 实际: {db_result['result']}"
    print("  [PASS] M2 检查完成 — 当天数据已到达")

    return {"monitor": "M2", "db": db_result}


def run_m3(storage: MySQLStorage, conn) -> dict:
    """M3: gmdb_plate_info 准确性检查。"""
    print("\n" + "=" * 60)
    print("M3: gmdb_plate_info 准确性检查")
    print("=" * 60)

    checker = AccuracyChecker(
        monitor_id=MonitorID.M3,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=storage,
    )

    checker.execute(CHECK_DATE, CHECK_TIME, 1)

    # 验证
    db_result = query_check_result(conn, "M3")
    details = query_accuracy_details(conn, "M3")

    print(f"\n  DB 结果: result={db_result['result'] if db_result else 'N/A'}")
    if details:
        print(f"  准确性明细 ({len(details)} 条):")
        for d in details:
            print(f"    - record_key={d['record_key']}, field={d['field_name']}, "
                  f"error_type={d['error_type']}, error_value={d['error_value']}")

    assert db_result is not None, "M3 结果应已写入数据库"
    # 预期 FAIL，因为有 BAD1 (stkcode不匹配正则, mst_type不在枚举) 和 049999 (std_stkcode不匹配正则)
    assert db_result["result"] == CheckResult.FAIL, f"M3 应为 FAIL (有异常数据), 实际: {db_result['result']}"
    assert len(details) > 0, "M3 应有准确性明细记录"
    print("  [PASS] M3 检查完成 — 检测到数据异常")

    return {"monitor": "M3", "db": db_result, "details": details}


def run_m4(storage: MySQLStorage, conn) -> dict:
    """M4: ads_fin_index_compn_stock_interface_ds 完整性检查。"""
    print("\n" + "=" * 60)
    print("M4: ads_fin_index_compn_stock_interface_ds 完整性检查")
    print("=" * 60)

    checker = CompletenessChecker(
        monitor_id=MonitorID.M4,
        table="ads_fin_index_compn_stock_interface_ds",
        key_field="compn_stock_code",
        mysql_storage=storage,
        group_field="stkcode",
    )

    # 标记 Pulsar 未失败，跳过 _prepare（快照已手动插入）
    checker._pulsar_failed = False
    checker._pulsar_error = ""

    # 直接调用 _check → _record → _alert
    result = checker._check(CHECK_DATE, CHECK_TIME, 1)
    checker._record(CHECK_DATE, CHECK_TIME, 1, result)
    checker._alert(CHECK_DATE, CHECK_TIME, 1, result)

    # 验证
    db_result = query_check_result(conn, "M4")
    print(f"\n  状态: {result['status']}")
    print(f"  线上表 compn_stock_code 总数: {result['online_count']}")
    print(f"  快照表 compn_stock_code 总数: {result['snapshot_count']}")
    print(f"  missing: {result['missing']}")
    print(f"  extra: {result['extra']}")
    if result.get("group_details"):
        print(f"  分组差异 ({len(result['group_details'])} 个组):")
        for g in result["group_details"]:
            print(f"    - {g['group_key']}: missing={g['missing']}, extra={g['extra']}, "
                  f"online={g['online_count']}, snapshot={g['snapshot_count']}")

    assert db_result is not None, "M4 结果应已写入数据库"
    print("  [PASS] M4 检查完成")

    return {"monitor": "M4", "result": result, "db": db_result}


def run_m5(storage: MySQLStorage, conn) -> dict:
    """M5: ads_fin_index_compn_stock_interface_ds 及时性检查。"""
    print("\n" + "=" * 60)
    print("M5: ads_fin_index_compn_stock_interface_ds 及时性检查")
    print("=" * 60)

    checker = TimelinessChecker(
        monitor_id=MonitorID.M5,
        table="ads_fin_index_compn_stock_interface_ds",
        mysql_storage=storage,
    )

    checker.execute(CHECK_DATE, CHECK_TIME, 1)

    # 验证
    db_result = query_check_result(conn, "M5")
    print(f"\n  DB 结果: result={db_result['result'] if db_result else 'N/A'}")

    assert db_result is not None, "M5 结果应已写入数据库"
    assert db_result["result"] == CheckResult.PASS, f"M5 应为 PASS, 实际: {db_result['result']}"
    print("  [PASS] M5 检查完成 — 当天数据已到达")

    return {"monitor": "M5", "db": db_result}


def run_m6(storage: MySQLStorage, conn) -> dict:
    """M6: ads_fin_index_compn_stock_interface_ds 准确性检查。"""
    print("\n" + "=" * 60)
    print("M6: ads_fin_index_compn_stock_interface_ds 准确性检查")
    print("=" * 60)

    checker = AccuracyChecker(
        monitor_id=MonitorID.M6,
        table="ads_fin_index_compn_stock_interface_ds",
        key_field="compn_stock_code",
        mysql_storage=storage,
    )

    checker.execute(CHECK_DATE, CHECK_TIME, 1)

    # 验证
    db_result = query_check_result(conn, "M6")
    details = query_accuracy_details(conn, "M6")

    print(f"\n  DB 结果: result={db_result['result'] if db_result else 'N/A'}")
    if details:
        print(f"  准确性明细 ({len(details)} 条):")
        for d in details:
            print(f"    - record_key={d['record_key']}, field={d['field_name']}, "
                  f"error_type={d['error_type']}, error_value={d['error_value']}")

    assert db_result is not None, "M6 结果应已写入数据库"
    # 预期 FAIL，因为有 compn_stock_name="" (NULL_VALUE) 和 index_name="" (NULL_VALUE)
    assert db_result["result"] == CheckResult.FAIL, f"M6 应为 FAIL, 实际: {db_result['result']}"
    assert len(details) > 0, "M6 应有准确性明细记录"
    print("  [PASS] M6 检查完成 — 检测到数据异常")

    return {"monitor": "M6", "db": db_result, "details": details}


# ═══════════════════════════════════════════════════════════
# 5. 第二轮检查（验证快照共享 + upsert 幂等性）
# ═══════════════════════════════════════════════════════════

def run_round2(storage: MySQLStorage, conn) -> None:
    """第二轮检查：验证快照共享和 upsert 幂等性。"""
    print("\n" + "=" * 60)
    print("第二轮检查：验证快照共享 + upsert 幂等性")
    print("=" * 60)

    # 查看快照表中现有数据量
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM dqm_security_info_snapshot WHERE check_date = %s", (CHECK_DATE,))
    snapshot_count_before = cur.fetchone()[0]
    print(f"  第二轮前快照数: {snapshot_count_before}")

    # M1 第二轮：模拟 _prepare 检测到快照已存在则跳过
    checker_m1 = CompletenessChecker(
        monitor_id=MonitorID.M1,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=storage,
    )

    # 通过 mock _prepare 模拟"快照已存在，跳过采集"
    from unittest.mock import patch as mock_patch

    def mock_prepare_skip(cd, ct, cr):
        """模拟 _prepare：快照已存在，跳过采集。"""
        checker_m1._pulsar_failed = False
        checker_m1._pulsar_error = ""

    with mock_patch.object(checker_m1, "_prepare", side_effect=mock_prepare_skip):
        checker_m1.execute(CHECK_DATE, CHECK_TIME, 2)

    # 验证快照数不变
    cur.execute("SELECT COUNT(*) as cnt FROM dqm_security_info_snapshot WHERE check_date = %s", (CHECK_DATE,))
    snapshot_count_after = cur.fetchone()[0]
    print(f"  第二轮后快照数: {snapshot_count_after}")
    assert snapshot_count_after == snapshot_count_before, "第二轮不应新增快照（共享已有数据）"

    # 验证 M1 第二轮结果写入
    cur.execute(
        "SELECT * FROM dqm_check_result WHERE check_date = %s AND check_round = 2 AND monitor_id = 'M1'",
        (CHECK_DATE,),
    )
    row = cur.fetchone()
    assert row is not None, "M1 第二轮结果应已写入"

    # 验证 M1 第一轮和第二轮结果共存
    cur.execute(
        "SELECT check_round, result FROM dqm_check_result WHERE check_date = %s AND monitor_id = 'M1' ORDER BY check_round",
        (CHECK_DATE,),
    )
    rows = cur.fetchall()
    print(f"  M1 各轮结果: {[(r[0], r[1]) for r in rows]}")
    assert len(rows) == 2, "M1 应有 2 轮检查结果"

    cur.close()
    print("  [PASS] 第二轮检查完成 — 快照共享 + upsert 幂等性验证通过")


# ═══════════════════════════════════════════════════════════
# 6. 汇总展示
# ═══════════════════════════════════════════════════════════

def print_summary(conn) -> None:
    """打印所有检查结果汇总。"""
    print("\n" + "=" * 60)
    print("检查结果汇总")
    print("=" * 60)

    cur = conn.cursor(DictCursor)
    cur.execute(
        "SELECT monitor_id, dimension, check_round, result, detail "
        "FROM dqm_check_result WHERE check_date = %s ORDER BY monitor_id, check_round",
        (CHECK_DATE,),
    )
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print("  (无检查结果)")
        return

    print(f"\n  {'监控项':<8} {'维度':<8} {'轮次':<6} {'结果':<8} 详情摘要")
    print(f"  {'─'*8} {'─'*8} {'─'*6} {'─'*8} {'─'*30}")
    for r in rows:
        import json
        detail = json.loads(r["detail"]) if r["detail"] else {}
        summary = ""
        if r["monitor_id"] in ("M1", "M4"):
            summary = f"online={detail.get('online_count', '?')}, snapshot={detail.get('snapshot_count', '?')}"
            if detail.get("missing"):
                summary += f", missing={len(detail['missing'])}"
            if detail.get("extra"):
                summary += f", extra={len(detail['extra'])}"
        elif r["monitor_id"] in ("M2", "M5"):
            summary = f"total_count={detail.get('total_count', '?')}"
        elif r["monitor_id"] in ("M3", "M6"):
            summary = f"total={detail.get('total', '?')}, errors={detail.get('errors', '?')}"
        print(f"  {r['monitor_id']:<8} {r['dimension']:<8} {r['check_round']:<6} {r['result']:<8} {summary}")

    # 准确性明细汇总
    cur2 = conn.cursor()
    cur2.execute(
        "SELECT monitor_id, COUNT(*) as cnt FROM dqm_accuracy_detail "
        "WHERE check_date = %s GROUP BY monitor_id",
        (CHECK_DATE,),
    )
    detail_rows = cur2.fetchall()
    cur2.close()
    if detail_rows:
        print(f"\n  准确性明细:")
        for dr in detail_rows:
            print(f"    {dr[0]}: {dr[1]} 条异常记录")


# ═══════════════════════════════════════════════════════════
# 7. 主流程
# ═══════════════════════════════════════════════════════════

def main():
    setup_logger()
    log = get_logger("TEST", "E2E")

    print("╔══════════════════════════════════════════════════════╗")
    print("║  数据质量监控 本地端到端测试                         ║")
    print(f"║  检查日期: {CHECK_DATE}                             ║")
    print(f"║  数据库: {TEST_DB_HOST}:{TEST_DB_PORT}/{TEST_DB_NAME}          ║")
    print("╚══════════════════════════════════════════════════════╝")

    conn = get_raw_connection()
    start_patching()
    storage = get_mysql_storage()

    try:
        # ── Step 1: 创建表 ──
        print("\n[Step 1] 创建表...")
        setup_tables(conn)

        # ── Step 2: 插入业务表模拟数据 ──
        print("\n[Step 2] 插入业务表模拟数据...")
        insert_business_data(conn)

        # ── Step 3: 插入快照模拟数据 ──
        print("\n[Step 3] 插入快照模拟数据（替代 Pulsar 采集）...")
        insert_snapshot_data(conn, check_round=1)

        # ── Step 4: 执行 M1-M6 检查 ──
        print("\n[Step 4] 执行 M1-M6 检查...")

        results = {}
        results["M1"] = run_m1(storage, conn)
        results["M2"] = run_m2(storage, conn)
        results["M3"] = run_m3(storage, conn)
        results["M4"] = run_m4(storage, conn)
        results["M5"] = run_m5(storage, conn)
        results["M6"] = run_m6(storage, conn)

        # ── Step 5: 第二轮检查 ──
        print("\n[Step 5] 第二轮检查（验证快照共享 + 幂等性）...")
        run_round2(storage, conn)

        # ── Step 6: 汇总 ──
        print_summary(conn)

        print("\n" + "╔" + "═" * 58 + "╗")
        print("║  所有测试通过！M1-M6 监控项全部正常工作。             ║")
        print("╚" + "═" * 58 + "╝")

    except Exception as e:
        log.critical(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        stop_patching()
        storage.close()
        conn.close()


if __name__ == "__main__":
    main()
