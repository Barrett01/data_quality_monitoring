"""M4 完整性检查器单元测试。

测试 CompletenessChecker 针对 M4 (ads_fin_index_compn_stock_interface_ds compn_stock_code)
的分组比对逻辑：
1. _filter_messages — M4 消息过滤（PLATE_STOCKS）
2. _check — 分组集合比对逻辑（PASS/FAIL/NODATA/SKIP）
3. _check_grouped — 按 stkcode 分组后比对 compn_stock_code
4. _record — 分组详情记录
5. _alert — 分组失败告警
6. 初始化 — group_field 参数
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from config.constants import CheckResult, Dimension, MonitorID
from src.dqm.checkers.completeness import CompletenessChecker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_mysql_storage():
    """模拟 MySQLStorage。"""
    return MagicMock()


@pytest.fixture
def m4_checker(mock_mysql_storage):
    """构造 M4 CompletenessChecker 实例（带 group_field）。"""
    return CompletenessChecker(
        monitor_id=MonitorID.M4,
        table="ads_fin_index_compn_stock_interface_ds",
        key_field="compn_stock_code",
        mysql_storage=mock_mysql_storage,
        group_field="stkcode",
    )


@pytest.fixture
def m1_checker(mock_mysql_storage):
    """构造 M1 CompletenessChecker 实例（无 group_field）。"""
    return CompletenessChecker(
        monitor_id=MonitorID.M1,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 22), datetime(2026, 4, 22, 8, 42, 0), 1


# ---------------------------------------------------------------------------
# 1. _filter_messages 测试 — M4
# ---------------------------------------------------------------------------

class TestFilterMessagesM4:
    """M4 消息过滤逻辑测试。"""

    def test_filter_m4_plate_stocks(self, m4_checker):
        """M4 应保留 PLATE_STOCKS 类型消息。"""
        messages = [{"mst_type": "PLATE_STOCKS", "compn_stock_code": "SH600000"}]
        result = m4_checker._filter_messages(messages)
        assert len(result) == 1
        assert result[0]["compn_stock_code"] == "SH600000"

    def test_filter_m4_exclude_industry(self, m4_checker):
        """M4 应排除 INDUSTRY_PLATE_INFO 类型消息（属于 M1）。"""
        messages = [{"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"}]
        result = m4_checker._filter_messages(messages)
        assert len(result) == 0

    def test_filter_m4_mixed_messages(self, m4_checker):
        """M4 应从混合消息中仅保留 PLATE_STOCKS。"""
        messages = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"},
            {"mst_type": "PLATE_STOCKS", "stkcode": "0001.BK", "compn_stock_code": "SH600000"},
            {"mst_type": "REGION_PLATE_INFO", "stkcode": "BK0002"},
            {"mst_type": "PLATE_STOCKS", "stkcode": "0477.BK", "compn_stock_code": "SZ000001"},
        ]
        result = m4_checker._filter_messages(messages)
        assert len(result) == 2
        assert all(r["mst_type"] == "PLATE_STOCKS" for r in result)

    def test_filter_m4_empty_list(self, m4_checker):
        """空消息列表应返回空列表。"""
        assert m4_checker._filter_messages([]) == []


# ---------------------------------------------------------------------------
# 2. _check 分组比对测试
# ---------------------------------------------------------------------------

class TestCheckGrouped:
    """M4 分组比对逻辑测试。"""

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_pass(self, mock_online, mock_snapshot, m4_checker, check_params):
        """所有分组的 compn_stock_code 完全一致 → PASS。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000", "SH600001"],
            "0477.BK": ["SZ000001", "SH600519"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000", "SH600001"],
            "0477.BK": ["SZ000001", "SH600519"],
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.PASS
        assert result["missing"] == []
        assert result["extra"] == []
        assert result["group_details"] == []

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_fail_missing(self, mock_online, mock_snapshot, m4_checker, check_params):
        """某分组中快照有但线上无 → FAIL, 有 missing。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000", "SH600001"],
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert "SH600001" in result["missing"]
        assert len(result["group_details"]) == 1
        assert result["group_details"][0]["group_key"] == "0001.BK"

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_fail_extra(self, mock_online, mock_snapshot, m4_checker, check_params):
        """某分组中线上有但快照无 → FAIL, 有 extra。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000", "SH600001"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000"],
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert "SH600001" in result["extra"]

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_fail_multiple_groups(self, mock_online, mock_snapshot, m4_checker, check_params):
        """多个分组都有差异 → FAIL, 多个 group_details。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000"],
            "0477.BK": ["SZ000001", "SH600519"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000", "SH600001"],  # missing SH600001
            "0477.BK": ["SZ000001"],               # extra SH600519
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert len(result["group_details"]) == 2
        group_keys = {g["group_key"] for g in result["group_details"]}
        assert "0001.BK" in group_keys
        assert "0477.BK" in group_keys

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_nodata(self, mock_online, mock_snapshot, m4_checker, check_params):
        """快照为空 → NODATA。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000"],
        }
        mock_snapshot.return_value = {}

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.NODATA

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_nodata_all_empty(self, mock_online, mock_snapshot, m4_checker, check_params):
        """线上和快照都为空 → NODATA。"""
        mock_online.return_value = {}
        mock_snapshot.return_value = {}

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.NODATA

    def test_check_grouped_skip(self, m4_checker, check_params):
        """Pulsar 采集失败 → SKIP。"""
        m4_checker._pulsar_failed = True
        m4_checker._pulsar_error = "Connection refused"

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.SKIP
        assert result["group_details"] == []

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_online_only_group(self, mock_online, mock_snapshot, m4_checker, check_params):
        """线上有某个分组但快照没有该分组 → FAIL, 整组都是 extra。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000"],
            "0477.BK": ["SZ000001"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000"],
            # 0477.BK 完全缺失
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert "SZ000001" in result["extra"]
        gd_0477 = [g for g in result["group_details"] if g["group_key"] == "0477.BK"]
        assert len(gd_0477) == 1
        assert "SZ000001" in gd_0477[0]["extra"]

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_check_grouped_snapshot_only_group(self, mock_online, mock_snapshot, m4_checker, check_params):
        """快照有某个分组但线上没有该分组 → FAIL, 整组都是 missing。"""
        mock_online.return_value = {
            "0001.BK": ["SH600000"],
        }
        mock_snapshot.return_value = {
            "0001.BK": ["SH600000"],
            "0477.BK": ["SZ000001"],  # 线上没有 0477.BK
        }

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert "SZ000001" in result["missing"]


# ---------------------------------------------------------------------------
# 3. M1 vs M4 模式切换测试
# ---------------------------------------------------------------------------

class TestModeSwitch:
    """验证 M1（全局比对）和 M4（分组比对）模式切换正确。"""

    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    @patch.object(CompletenessChecker, "_query_online_keys")
    def test_m1_uses_global_check(self, mock_online, mock_snapshot, m1_checker, check_params):
        """M1 应使用全局比对（_query_online_keys）。"""
        mock_online.return_value = ["BK0001", "BK0002"]
        mock_snapshot.return_value = ["BK0001", "BK0002"]

        result = m1_checker._check(*check_params)

        assert result["status"] == CheckResult.PASS
        mock_online.assert_called_once()
        mock_snapshot.assert_called_once()

    @patch.object(CompletenessChecker, "_query_snapshot_grouped_keys")
    @patch.object(CompletenessChecker, "_query_online_grouped_keys")
    def test_m4_uses_grouped_check(self, mock_online, mock_snapshot, m4_checker, check_params):
        """M4 应使用分组比对（_query_online_grouped_keys）。"""
        mock_online.return_value = {"0001.BK": ["SH600000"]}
        mock_snapshot.return_value = {"0001.BK": ["SH600000"]}

        result = m4_checker._check(*check_params)

        assert result["status"] == CheckResult.PASS
        mock_online.assert_called_once()
        mock_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# 4. _query_online_grouped_keys / _query_snapshot_grouped_keys 测试
# ---------------------------------------------------------------------------

class TestQueryGroupedKeys:
    """分组查询方法测试。"""

    def test_query_online_grouped_keys(self, m4_checker, mock_mysql_storage, check_params):
        """_query_online_grouped_keys 应返回按 group_field 分组的结果。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000"},
            {"stkcode": "0001.BK", "compn_stock_code": "SH600001"},
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001"},
        ]

        result = m4_checker._query_online_grouped_keys(check_params[0])

        assert "0001.BK" in result
        assert "0477.BK" in result
        assert set(result["0001.BK"]) == {"SH600000", "SH600001"}
        assert result["0477.BK"] == ["SZ000001"]

    def test_query_online_grouped_keys_dedup(self, m4_checker, mock_mysql_storage, check_params):
        """_query_online_grouped_keys 应对每个分组内的值去重。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000"},
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000"},  # 重复
        ]

        result = m4_checker._query_online_grouped_keys(check_params[0])

        assert result["0001.BK"] == ["SH600000"]

    def test_query_snapshot_grouped_keys(self, m4_checker, mock_mysql_storage, check_params):
        """_query_snapshot_grouped_keys 应返回按 group_field 分组的结果。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "0001.BK", "compn_stock_code": "SH600000"},
            {"stkcode": "0477.BK", "compn_stock_code": "SZ000001"},
        ]

        result = m4_checker._query_snapshot_grouped_keys(check_params[0], check_params[2])

        assert "0001.BK" in result
        assert "0477.BK" in result
        sql = mock_mysql_storage.execute_query.call_args[0][0]
        assert "dqm_security_info_snapshot" in sql
        assert "check_date" in sql

    def test_query_online_grouped_keys_raises_on_error(self, m4_checker, mock_mysql_storage, check_params):
        """查询失败时应抛出异常。"""
        mock_mysql_storage.execute_query.side_effect = Exception("DB Error")

        with pytest.raises(Exception, match="DB Error"):
            m4_checker._query_online_grouped_keys(check_params[0])


# ---------------------------------------------------------------------------
# 5. _record 分组详情测试
# ---------------------------------------------------------------------------

class TestRecordGrouped:
    """M4 分组详情记录测试。"""

    def test_record_with_group_details(self, m4_checker, mock_mysql_storage, check_params):
        """_record 应将 group_details 写入 detail JSON。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.FAIL,
            "missing": ["SH600001"],
            "extra": [],
            "online_count": 1,
            "snapshot_count": 2,
            "group_details": [
                {
                    "group_key": "0001.BK",
                    "missing": ["SH600001"],
                    "extra": [],
                    "online_count": 1,
                    "snapshot_count": 2,
                }
            ],
        }

        m4_checker._record(check_date, check_time, check_round, result)

        call_args = mock_mysql_storage.execute_update.call_args
        detail = json.loads(call_args[0][1][5])
        assert "group_details" in detail
        assert len(detail["group_details"]) == 1
        assert detail["group_details"][0]["group_key"] == "0001.BK"

    def test_record_without_group_details(self, m1_checker, mock_mysql_storage, check_params):
        """M1 的 _record 不应包含 group_details。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 5,
            "snapshot_count": 5,
            "group_details": [],
        }

        m1_checker._record(check_date, check_time, check_round, result)

        call_args = mock_mysql_storage.execute_update.call_args
        detail = json.loads(call_args[0][1][5])
        assert "group_details" not in detail


# ---------------------------------------------------------------------------
# 6. _alert 分组失败告警测试
# ---------------------------------------------------------------------------

class TestAlertGrouped:
    """M4 分组失败告警日志输出测试。"""

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_fail_grouped(self, mock_get_logger, m4_checker, check_params):
        """M4 FAIL 时应使用 format_completeness_fail_grouped。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.FAIL,
            "missing": ["SH600001"],
            "extra": [],
            "online_count": 1,
            "snapshot_count": 2,
            "group_details": [
                {
                    "group_key": "0001.BK",
                    "missing": ["SH600001"],
                    "extra": [],
                    "online_count": 1,
                    "snapshot_count": 2,
                }
            ],
        }

        m4_checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        msg = mock_log.error.call_args[0][0]
        assert "成分股代码不一致" in msg
        assert "0001.BK" in msg

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_pass_grouped(self, mock_get_logger, m4_checker, check_params):
        """M4 PASS 时应输出 INFO 日志（和 M1 相同格式）。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 2,
            "snapshot_count": 2,
            "group_details": [],
        }

        m4_checker._alert(*check_params, result)

        mock_log.info.assert_called_once()
        msg = mock_log.info.call_args[0][0]
        assert "完全一致" in msg


# ---------------------------------------------------------------------------
# 7. M4 初始化测试
# ---------------------------------------------------------------------------

class TestInitM4:
    """M4 CompletenessChecker 初始化测试。"""

    def test_init_group_field(self, mock_mysql_storage):
        """验证 M4 初始化时 group_field 正确设置。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M4,
            table="ads_fin_index_compn_stock_interface_ds",
            key_field="compn_stock_code",
            mysql_storage=mock_mysql_storage,
            group_field="stkcode",
        )

        assert checker.monitor_id == MonitorID.M4
        assert checker.dimension == Dimension.COMPLETENESS
        assert checker.table == "ads_fin_index_compn_stock_interface_ds"
        assert checker.key_field == "compn_stock_code"
        assert checker.group_field == "stkcode"

    def test_init_m1_no_group_field(self, mock_mysql_storage):
        """验证 M1 初始化时 group_field 为 None。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table="gmdb_plate_info",
            key_field="stkcode",
            mysql_storage=mock_mysql_storage,
        )

        assert checker.group_field is None
