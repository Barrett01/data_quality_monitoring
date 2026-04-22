"""M5 及时性检查器单元测试。

测试 TimelinessChecker 针对 M5 (ads_fin_index_compn_stock_interface_ds send_date) 的所有核心逻辑：
1. _date_to_send_date — 日期格式转换（继承自 TimelinessChecker）
2. _check — M5 及时性比对逻辑（PASS/FAIL，使用 _query_count 而非 _query_by_mst_type）
3. _record — 检查结果记录
4. _alert — M5 告警日志输出（使用 format_timeliness_fail_m5 格式）
5. _prepare — 无操作
6. execute — 模板方法整体流程
7. M5 vs M2 行为差异验证
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from config.constants import CheckResult, Dimension, MonitorID
from src.dqm.checkers.timeliness import TimelinessChecker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_mysql_storage():
    """模拟 MySQLStorage。"""
    return MagicMock()


@pytest.fixture
def m5_checker(mock_mysql_storage):
    """构造 M5 TimelinessChecker 实例。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M5,
        table="ads_fin_index_compn_stock_interface_ds",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def m2_checker(mock_mysql_storage):
    """构造 M2 TimelinessChecker 实例，用于对比测试。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M2,
        table="gmdb_plate_info",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 22), datetime(2026, 4, 22, 8, 42, 0), 1


# ---------------------------------------------------------------------------
# 1. _check — M5 及时性比对逻辑
# ---------------------------------------------------------------------------

class TestCheckM5:
    """测试 M5 _check 方法的核心比对逻辑。"""

    def test_check_pass_with_data(self, m5_checker, mock_mysql_storage, check_params):
        """场景：存在当天数据 → PASS。"""
        mock_mysql_storage.execute_query.return_value = [
            {"cnt": 7},
        ]
        result = m5_checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 7

    def test_check_fail_no_data(self, m5_checker, mock_mysql_storage, check_params):
        """场景：无当天数据 → FAIL。"""
        mock_mysql_storage.execute_query.return_value = [
            {"cnt": 0},
        ]
        result = m5_checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0
        assert result["details"] == []

    def test_check_fail_empty_result(self, m5_checker, mock_mysql_storage, check_params):
        """场景：查询返回空结果集 → FAIL。"""
        mock_mysql_storage.execute_query.return_value = []
        result = m5_checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0

    def test_check_pass_large_count(self, m5_checker, mock_mysql_storage, check_params):
        """场景：大量当天数据 → PASS。"""
        mock_mysql_storage.execute_query.return_value = [
            {"cnt": 5000},
        ]
        result = m5_checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 5000

    def test_check_sends_correct_send_date(self, m5_checker, mock_mysql_storage, check_params):
        """验证 M5 查询使用了正确的 send_date 值。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 1}]
        m5_checker._check(*check_params)
        call_args = mock_mysql_storage.execute_query.call_args
        # M5 使用 _query_count，参数为 (send_date,)
        assert call_args[0][1] == (20260422,)

    def test_check_result_contains_send_date(self, m5_checker, mock_mysql_storage, check_params):
        """M5 检查结果应包含 send_date。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 3}]
        result = m5_checker._check(*check_params)
        assert result["send_date"] == 20260422

    def test_check_result_contains_check_date(self, m5_checker, mock_mysql_storage, check_params):
        """M5 检查结果应包含 check_date 字符串。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 3}]
        result = m5_checker._check(*check_params)
        assert result["check_date"] == "2026-04-22"


# ---------------------------------------------------------------------------
# 2. M5 vs M2 行为差异验证
# ---------------------------------------------------------------------------

class TestM5vsM2Difference:
    """验证 M5 和 M2 使用不同的查询策略。"""

    def test_m5_uses_query_count(self, m5_checker, mock_mysql_storage, check_params):
        """M5 使用 _query_count（简单 COUNT 查询）。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 5}]
        m5_checker._check(*check_params)

        # M5 的 SQL 应该是简单的 COUNT(*) 查询，无 mst_type 相关
        call_args = mock_mysql_storage.execute_query.call_args
        sql = call_args[0][0]
        assert "mst_type" not in sql
        assert "COUNT" in sql

    def test_m2_uses_query_by_mst_type(self, m2_checker, mock_mysql_storage, check_params):
        """M2 使用 _query_by_mst_type（按 mst_type 分组查询）。"""
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 85},
        ]
        m2_checker._check(*check_params)

        # M2 的 SQL 应该包含 mst_type 字段
        call_args = mock_mysql_storage.execute_query.call_args
        sql = call_args[0][0]
        assert "mst_type" in sql

    def test_m5_details_format(self, m5_checker, mock_mysql_storage, check_params):
        """M5 的 details 格式：空 mst_type + count。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 7}]
        result = m5_checker._check(*check_params)
        assert len(result["details"]) == 1
        assert result["details"][0]["mst_type"] == ""
        assert result["details"][0]["count"] == 7

    def test_m2_details_format(self, m2_checker, mock_mysql_storage, check_params):
        """M2 的 details 格式：按 mst_type 分组。"""
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 85},
            {"mst_type": "REGION_PLATE_INFO", "cnt": 30},
        ]
        result = m2_checker._check(*check_params)
        assert len(result["details"]) == 2
        assert result["details"][0]["mst_type"] == "INDUSTRY_PLATE_INFO"
        assert result["details"][0]["count"] == 85


# ---------------------------------------------------------------------------
# 3. _record — 检查结果记录
# ---------------------------------------------------------------------------

class TestRecordM5:
    """测试 M5 _record 方法是否正确写入检查结果。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_record_pass(self, mock_get_logger, m5_checker, check_params):
        """PASS 结果写入检查结果表。"""
        mock_repo = MagicMock()
        m5_checker._check_result_repo = mock_repo

        result = {
            "status": CheckResult.PASS,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 7,
            "details": [{"mst_type": "", "count": 7}],
        }
        m5_checker._record(*check_params, result)

        mock_repo.upsert.assert_called_once()
        call_kwargs = mock_repo.upsert.call_args
        assert call_kwargs[1]["monitor_id"] == MonitorID.M5
        assert call_kwargs[1]["dimension"] == Dimension.TIMELINESS
        assert call_kwargs[1]["result"] == CheckResult.PASS
        detail = json.loads(call_kwargs[1]["detail"])
        assert detail["total_count"] == 7
        assert detail["send_date"] == 20260422

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_record_fail(self, mock_get_logger, m5_checker, check_params):
        """FAIL 结果写入检查结果表。"""
        mock_repo = MagicMock()
        m5_checker._check_result_repo = mock_repo

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
        }
        m5_checker._record(*check_params, result)

        call_kwargs = mock_repo.upsert.call_args
        assert call_kwargs[1]["result"] == CheckResult.FAIL
        detail = json.loads(call_kwargs[1]["detail"])
        assert detail["total_count"] == 0


# ---------------------------------------------------------------------------
# 4. _alert — M5 告警日志输出
# ---------------------------------------------------------------------------

class TestAlertM5:
    """测试 M5 _alert 方法的告警日志输出。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_pass(self, mock_get_logger, m5_checker, check_params):
        """PASS → 输出 INFO 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.PASS,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 7,
            "details": [],
        }
        m5_checker._alert(*check_params, result)

        mock_log.info.assert_called_once()
        assert "当天数据已到达" in mock_log.info.call_args[0][0]

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_fail_m5_format(self, mock_get_logger, m5_checker, check_params):
        """M5 FAIL → 输出 ERROR 日志，使用 PLATE_STOCKS 格式。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
        }
        m5_checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        alert_msg = mock_log.error.call_args[0][0]
        assert "PLATE_STOCKS 数据未及时到达" in alert_msg

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_fail_m2_format(self, mock_get_logger, m2_checker, check_params):
        """M2 FAIL → 输出 ERROR 日志，使用通用格式（非 PLATE_STOCKS）。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
        }
        m2_checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        alert_msg = mock_log.error.call_args[0][0]
        assert "PLATE_STOCKS" not in alert_msg
        assert "无当天数据" in alert_msg

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_error(self, mock_get_logger, m5_checker, check_params):
        """ERROR → 输出 CRITICAL 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.ERROR,
            "check_date": "2026-04-22",
            "send_date": 20260422,
            "total_count": 0,
            "details": [],
            "error": "MySQL connection failed",
        }
        m5_checker._alert(*check_params, result)

        mock_log.critical.assert_called_once()
        assert "及时性检查异常" in mock_log.critical.call_args[0][0]


# ---------------------------------------------------------------------------
# 5. _prepare — 无操作
# ---------------------------------------------------------------------------

class TestPrepareM5:
    """测试 _prepare 方法（M5 不需要 Pulsar 采集）。"""

    def test_prepare_is_noop(self, m5_checker, check_params):
        """_prepare 应该不做任何事。"""
        result = m5_checker._prepare(*check_params)
        assert result is None


# ---------------------------------------------------------------------------
# 6. execute — 模板方法整体流程
# ---------------------------------------------------------------------------

class TestExecuteM5:
    """测试 M5 execute 模板方法。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_execute_pass_flow(self, mock_get_logger, m5_checker, mock_mysql_storage, check_params):
        """完整 execute 流程：PASS。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = [
            {"cnt": 7},
        ]

        m5_checker.execute(*check_params)

        info_calls = [c for c in mock_log.info.call_args_list]
        assert any("当天数据已到达" in str(c) for c in info_calls)

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_execute_fail_flow(self, mock_get_logger, m5_checker, mock_mysql_storage, check_params):
        """完整 execute 流程：FAIL（无当天数据）。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = [{"cnt": 0}]

        m5_checker.execute(*check_params)

        error_calls = [c for c in mock_log.error.call_args_list]
        assert any("PLATE_STOCKS 数据未及时到达" in str(c) for c in error_calls)


# ---------------------------------------------------------------------------
# 7. _query_count — M5 专用查询方法
# ---------------------------------------------------------------------------

class TestQueryCountM5:
    """测试 M5 使用的 _query_count 方法。"""

    def test_query_count_with_data(self, m5_checker, mock_mysql_storage):
        """查询到数据时返回正确格式。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 7}]
        result = m5_checker._query_count(20260422)
        assert len(result) == 1
        assert result[0]["mst_type"] == ""
        assert result[0]["count"] == 7

    def test_query_count_no_data(self, m5_checker, mock_mysql_storage):
        """查询不到数据时返回 count=0。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 0}]
        result = m5_checker._query_count(20260422)
        assert len(result) == 1
        assert result[0]["count"] == 0

    def test_query_count_empty_result(self, m5_checker, mock_mysql_storage):
        """查询返回空结果集时返回 count=0。"""
        mock_mysql_storage.execute_query.return_value = []
        result = m5_checker._query_count(20260422)
        assert len(result) == 1
        assert result[0]["count"] == 0

    def test_query_count_sql_format(self, m5_checker, mock_mysql_storage):
        """验证 SQL 语句不包含 mst_type。"""
        mock_mysql_storage.execute_query.return_value = [{"cnt": 1}]
        m5_checker._query_count(20260422)
        call_args = mock_mysql_storage.execute_query.call_args
        sql = call_args[0][0]
        assert "send_date" in sql
        assert "mst_type" not in sql


# ---------------------------------------------------------------------------
# 8. 初始化验证
# ---------------------------------------------------------------------------

class TestInitM5:
    """验证 M5 的初始化参数。"""

    def test_m5_table_name(self, m5_checker):
        """M5 应使用 ads_fin_index_compn_stock_interface_ds 表。"""
        assert m5_checker.table == "ads_fin_index_compn_stock_interface_ds"

    def test_m5_monitor_id(self, m5_checker):
        """M5 的 monitor_id 应为 M5。"""
        assert m5_checker.monitor_id == MonitorID.M5

    def test_m5_dimension(self, m5_checker):
        """M5 的维度应为及时性。"""
        assert m5_checker.dimension == Dimension.TIMELINESS
