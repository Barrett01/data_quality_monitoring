"""M2 及时性检查器单元测试。

测试 TimelinessChecker 针对 M2 (gmdb_plate_info send_date) 的所有核心逻辑：
1. _date_to_send_date — 日期格式转换
2. _check — 及时性比对逻辑（PASS/FAIL）
3. _record — 检查结果记录
4. _alert — 告警日志输出
5. _prepare — 无操作（及时性不需要 Pulsar）
6. execute — 模板方法整体流程
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

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
def checker(mock_mysql_storage):
    """构造 M2 TimelinessChecker 实例。"""
    return TimelinessChecker(
        monitor_id=MonitorID.M2,
        table="gmdb_plate_info",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 21), datetime(2026, 4, 21, 9, 0, 0), 1


# ---------------------------------------------------------------------------
# 1. _date_to_send_date — 日期格式转换
# ---------------------------------------------------------------------------

class TestDateToSendDate:
    """测试日期到 send_date 整数的转换。"""

    def test_normal_date(self):
        assert TimelinessChecker._date_to_send_date(date(2026, 4, 21)) == 20260421

    def test_year_boundary(self):
        assert TimelinessChecker._date_to_send_date(date(2025, 12, 31)) == 20251231

    def test_new_year(self):
        assert TimelinessChecker._date_to_send_date(date(2026, 1, 1)) == 20260101


# ---------------------------------------------------------------------------
# 2. _check — 及时性比对逻辑
# ---------------------------------------------------------------------------

class TestCheck:
    """测试 _check 方法的核心比对逻辑。"""

    def test_check_pass_with_data(self, checker, mock_mysql_storage, check_params):
        """场景：存在当天数据 → PASS。"""
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 85},
            {"mst_type": "REGION_PLATE_INFO", "cnt": 30},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 115
        assert len(result["details"]) == 2

    def test_check_fail_no_data(self, checker, mock_mysql_storage, check_params):
        """场景：无当天数据 → FAIL。"""
        mock_mysql_storage.execute_query.return_value = []
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["total_count"] == 0
        assert result["details"] == []

    def test_check_pass_single_mst_type(self, checker, mock_mysql_storage, check_params):
        """场景：只有一种 mst_type 有数据 → PASS。"""
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 50},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total_count"] == 50

    def test_check_sends_correct_send_date(self, checker, mock_mysql_storage, check_params):
        """验证查询使用了正确的 send_date 值。"""
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 10},
        ]
        checker._check(*check_params)
        # 验证 execute_query 被调用时 send_date 参数为 20260421
        call_args = mock_mysql_storage.execute_query.call_args
        assert call_args[0][1] == (20260421,)


# ---------------------------------------------------------------------------
# 3. _record — 检查结果记录
# ---------------------------------------------------------------------------

class TestRecord:
    """测试 _record 方法是否正确写入检查结果。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_record_pass(self, mock_get_logger, checker, check_params):
        """PASS 结果写入检查结果表。"""
        mock_repo = MagicMock()
        checker._check_result_repo = mock_repo

        result = {
            "status": CheckResult.PASS,
            "check_date": "2026-04-21",
            "send_date": 20260421,
            "total_count": 115,
            "details": [{"mst_type": "INDUSTRY_PLATE_INFO", "count": 85}],
        }
        checker._record(*check_params, result)

        mock_repo.upsert.assert_called_once()
        call_kwargs = mock_repo.upsert.call_args
        assert call_kwargs[1]["monitor_id"] == MonitorID.M2
        assert call_kwargs[1]["dimension"] == Dimension.TIMELINESS
        assert call_kwargs[1]["result"] == CheckResult.PASS
        detail = json.loads(call_kwargs[1]["detail"])
        assert detail["total_count"] == 115

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_record_fail(self, mock_get_logger, checker, check_params):
        """FAIL 结果写入检查结果表。"""
        mock_repo = MagicMock()
        checker._check_result_repo = mock_repo

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-21",
            "send_date": 20260421,
            "total_count": 0,
            "details": [],
        }
        checker._record(*check_params, result)

        call_kwargs = mock_repo.upsert.call_args
        assert call_kwargs[1]["result"] == CheckResult.FAIL
        detail = json.loads(call_kwargs[1]["detail"])
        assert detail["total_count"] == 0


# ---------------------------------------------------------------------------
# 4. _alert — 告警日志输出
# ---------------------------------------------------------------------------

class TestAlert:
    """测试 _alert 方法的告警日志输出。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_pass(self, mock_get_logger, checker, check_params):
        """PASS → 输出 INFO 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.PASS,
            "check_date": "2026-04-21",
            "send_date": 20260421,
            "total_count": 115,
            "details": [],
        }
        checker._alert(*check_params, result)

        mock_log.info.assert_called_once()
        assert "当天数据已到达" in mock_log.info.call_args[0][0]

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_fail(self, mock_get_logger, checker, check_params):
        """FAIL → 输出 ERROR 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.FAIL,
            "check_date": "2026-04-21",
            "send_date": 20260421,
            "total_count": 0,
            "details": [],
        }
        checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        assert "无当天数据" in mock_log.error.call_args[0][0]

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_alert_error(self, mock_get_logger, checker, check_params):
        """ERROR → 输出 CRITICAL 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.ERROR,
            "check_date": "2026-04-21",
            "send_date": 20260421,
            "total_count": 0,
            "details": [],
            "error": "MySQL connection failed",
        }
        checker._alert(*check_params, result)

        mock_log.critical.assert_called_once()
        assert "及时性检查异常" in mock_log.critical.call_args[0][0]


# ---------------------------------------------------------------------------
# 5. _prepare — 无操作
# ---------------------------------------------------------------------------

class TestPrepare:
    """测试 _prepare 方法（及时性不需要 Pulsar 采集）。"""

    def test_prepare_is_noop(self, checker, check_params):
        """_prepare 应该不做任何事。"""
        result = checker._prepare(*check_params)
        assert result is None


# ---------------------------------------------------------------------------
# 6. execute — 模板方法整体流程
# ---------------------------------------------------------------------------

class TestExecute:
    """测试 execute 模板方法。"""

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_execute_pass_flow(self, mock_get_logger, checker, mock_mysql_storage, check_params):
        """完整 execute 流程：PASS。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "cnt": 85},
        ]

        checker.execute(*check_params)

        # 验证最终调用了 info 日志（PASS 告警）
        info_calls = [c for c in mock_log.info.call_args_list]
        assert any("当天数据已到达" in str(c) for c in info_calls)

    @patch("src.dqm.checkers.timeliness.get_logger")
    def test_execute_fail_flow(self, mock_get_logger, checker, mock_mysql_storage, check_params):
        """完整 execute 流程：FAIL（无当天数据）。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = []

        checker.execute(*check_params)

        # 验证最终调用了 error 日志（FAIL 告警）
        error_calls = [c for c in mock_log.error.call_args_list]
        assert any("无当天数据" in str(c) for c in error_calls)
