"""M1 完整性检查器单元测试。

测试 CompletenessChecker 针对 M1 (gmdb_plate_info stkcode) 的所有核心逻辑：
1. _filter_messages — M1 消息过滤
2. _check — 集合比对逻辑（PASS/FAIL/NODATA/SKIP）
3. _record — 检查结果记录
4. _alert — 告警日志输出
5. _prepare — Pulsar 采集与快照写入
6. execute — 模板方法整体流程
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

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
def checker(mock_mysql_storage):
    """构造 M1 CompletenessChecker 实例。"""
    return CompletenessChecker(
        monitor_id=MonitorID.M1,
        table="gmdb_plate_info",
        key_field="stkcode",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 21), datetime(2026, 4, 21, 9, 0, 0), 1


# ---------------------------------------------------------------------------
# 1. _filter_messages 测试
# ---------------------------------------------------------------------------

class TestFilterMessages:
    """M1 消息过滤逻辑测试。"""

    def test_filter_m1_industry_plate(self, checker):
        """M1 应保留 INDUSTRY_PLATE_INFO 类型消息。"""
        messages = [{"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"}]
        result = checker._filter_messages(messages)
        assert len(result) == 1
        assert result[0]["stkcode"] == "BK0001"

    def test_filter_m1_region_plate(self, checker):
        """M1 应保留 REGION_PLATE_INFO 类型消息。"""
        messages = [{"mst_type": "REGION_PLATE_INFO", "stkcode": "BK0002"}]
        result = checker._filter_messages(messages)
        assert len(result) == 1

    def test_filter_m1_hotidea_plate(self, checker):
        """M1 应保留 HOTIDEA_PLATE_INFO 类型消息。"""
        messages = [{"mst_type": "HOTIDEA_PLATE_INFO", "stkcode": "BK0003"}]
        result = checker._filter_messages(messages)
        assert len(result) == 1

    def test_filter_m1_exclude_plate_stocks(self, checker):
        """M1 应排除 PLATE_STOCKS 类型消息（属于 M4）。"""
        messages = [{"mst_type": "PLATE_STOCKS", "compn_stock_code": "600000"}]
        result = checker._filter_messages(messages)
        assert len(result) == 0

    def test_filter_m1_exclude_unknown_type(self, checker):
        """M1 应排除未知类型消息。"""
        messages = [{"mst_type": "UNKNOWN_TYPE", "stkcode": "BK0099"}]
        result = checker._filter_messages(messages)
        assert len(result) == 0

    def test_filter_m1_mixed_messages(self, checker):
        """M1 应从混合消息中仅保留三种相关类型。"""
        messages = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"},
            {"mst_type": "PLATE_STOCKS", "compn_stock_code": "600000"},
            {"mst_type": "REGION_PLATE_INFO", "stkcode": "BK0002"},
            {"mst_type": "SOME_OTHER", "stkcode": "BK0099"},
            {"mst_type": "HOTIDEA_PLATE_INFO", "stkcode": "BK0003"},
        ]
        result = checker._filter_messages(messages)
        assert len(result) == 3
        assert [r["mst_type"] for r in result] == [
            "INDUSTRY_PLATE_INFO",
            "REGION_PLATE_INFO",
            "HOTIDEA_PLATE_INFO",
        ]

    def test_filter_m1_empty_mst_type(self, checker):
        """M1 应排除 mst_type 为空的消息。"""
        messages = [{"stkcode": "BK0001"}]
        result = checker._filter_messages(messages)
        assert len(result) == 0

    def test_filter_m1_empty_list(self, checker):
        """空消息列表应返回空列表。"""
        assert checker._filter_messages([]) == []


# ---------------------------------------------------------------------------
# 2. _check 测试 — 集合比对核心逻辑
# ---------------------------------------------------------------------------

class TestCheck:
    """完整性比对逻辑测试。"""

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_pass_when_identical(self, mock_snapshot, mock_online, checker, check_params):
        """线上表与快照完全一致 → PASS。"""
        mock_online.return_value = ["BK0001", "BK0002", "BK0003"]
        mock_snapshot.return_value = ["BK0001", "BK0002", "BK0003"]

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.PASS
        assert result["missing"] == []
        assert result["extra"] == []
        assert result["online_count"] == 3
        assert result["snapshot_count"] == 3

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_fail_when_missing(self, mock_snapshot, mock_online, checker, check_params):
        """快照有但线上无 → FAIL, 有 missing。"""
        mock_online.return_value = ["BK0001"]
        mock_snapshot.return_value = ["BK0001", "BK0002"]

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert result["missing"] == ["BK0002"]
        assert result["extra"] == []

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_fail_when_extra(self, mock_snapshot, mock_online, checker, check_params):
        """线上有但快照无 → FAIL, 有 extra。"""
        mock_online.return_value = ["BK0001", "BK0002"]
        mock_snapshot.return_value = ["BK0001"]

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert result["missing"] == []
        assert result["extra"] == ["BK0002"]

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_fail_when_both_diff(self, mock_snapshot, mock_online, checker, check_params):
        """同时存在 missing 和 extra → FAIL。"""
        mock_online.return_value = ["BK0001", "BK0003"]
        mock_snapshot.return_value = ["BK0001", "BK0002"]

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert result["missing"] == ["BK0002"]
        assert result["extra"] == ["BK0003"]

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_nodata_when_snapshot_empty(self, mock_snapshot, mock_online, checker, check_params):
        """快照为空 → NODATA。"""
        mock_online.return_value = ["BK0001"]
        mock_snapshot.return_value = []

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.NODATA

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_nodata_when_both_empty(self, mock_snapshot, mock_online, checker, check_params):
        """线上表和快照都为空 → NODATA（快照为空优先）。"""
        mock_online.return_value = []
        mock_snapshot.return_value = []

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.NODATA

    def test_check_skip_when_pulsar_failed(self, checker, check_params):
        """Pulsar 采集失败 → SKIP。"""
        checker._pulsar_failed = True
        checker._pulsar_error = "Connection refused"

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.SKIP
        assert result["error"] == "Connection refused"

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_result_sorted(self, mock_snapshot, mock_online, checker, check_params):
        """missing 和 extra 应排序输出。"""
        mock_online.return_value = ["BK0003", "BK0001"]
        mock_snapshot.return_value = ["BK0001", "BK0004", "BK0002"]

        result = checker._check(*check_params)

        assert result["missing"] == ["BK0002", "BK0004"]
        assert result["extra"] == ["BK0003"]

    @patch.object(CompletenessChecker, "_query_online_keys")
    @patch.object(CompletenessChecker, "_query_snapshot_keys")
    def test_check_pass_with_empty_online_and_nonempty_snapshot(self, mock_snapshot, mock_online, checker, check_params):
        """线上表为空但快照不为空 → FAIL (all missing)。"""
        mock_online.return_value = []
        mock_snapshot.return_value = ["BK0001", "BK0002"]

        result = checker._check(*check_params)

        assert result["status"] == CheckResult.FAIL
        assert result["missing"] == ["BK0001", "BK0002"]
        assert result["extra"] == []


# ---------------------------------------------------------------------------
# 3. _record 测试
# ---------------------------------------------------------------------------

class TestRecord:
    """检查结果记录测试。"""

    def test_record_calls_upsert(self, checker, mock_mysql_storage, check_params):
        """_record 应调用 CheckResultRepository.upsert。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 5,
            "snapshot_count": 5,
        }

        checker._record(check_date, check_time, check_round, result)

        # 验证 upsert 被调用
        mock_mysql_storage.execute_update.assert_called_once()
        call_args = mock_mysql_storage.execute_update.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "INSERT INTO dqm_check_result" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert params[0] == check_date
        assert params[1] == check_round
        assert params[2] == MonitorID.M1
        assert params[3] == Dimension.COMPLETENESS
        assert params[4] == CheckResult.PASS

        # 验证 detail JSON 内容
        detail = json.loads(params[5])
        assert detail["online_count"] == 5
        assert detail["snapshot_count"] == 5
        assert detail["missing"] == []
        assert detail["extra"] == []

    def test_record_truncates_long_lists(self, checker, mock_mysql_storage, check_params):
        """_record 中 missing/extra 列表应截断到前 50 个。"""
        check_date, check_time, check_round = check_params
        result = {
            "status": CheckResult.FAIL,
            "missing": [f"BK{i:04d}" for i in range(100)],
            "extra": [f"BK{i:04d}" for i in range(100, 200)],
            "online_count": 200,
            "snapshot_count": 200,
        }

        checker._record(check_date, check_time, check_round, result)

        call_args = mock_mysql_storage.execute_update.call_args
        params = call_args[0][1]
        detail = json.loads(params[5])
        assert len(detail["missing"]) == 50
        assert len(detail["extra"]) == 50


# ---------------------------------------------------------------------------
# 4. _alert 测试
# ---------------------------------------------------------------------------

class TestAlert:
    """告警日志输出测试。"""

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_pass(self, mock_get_logger, checker, check_params):
        """PASS 时应输出 INFO 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 10,
            "snapshot_count": 10,
        }

        checker._alert(*check_params, result)

        mock_get_logger.assert_called_with(MonitorID.M1, Dimension.COMPLETENESS)
        mock_log.info.assert_called_once()
        msg = mock_log.info.call_args[0][0]
        assert "完全一致" in msg
        assert "online=10" in msg

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_fail(self, mock_get_logger, checker, check_params):
        """FAIL 时应输出 ERROR 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.FAIL,
            "missing": ["BK0002"],
            "extra": ["BK0003"],
            "online_count": 3,
            "snapshot_count": 3,
        }

        checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        msg = mock_log.error.call_args[0][0]
        assert "不一致" in msg
        assert "BK0002" in msg

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_skip(self, mock_get_logger, checker, check_params):
        """SKIP 时应输出 WARNING 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.SKIP,
            "error": "Connection refused",
        }

        checker._alert(*check_params, result)

        mock_log.warning.assert_called_once()
        msg = mock_log.warning.call_args[0][0]
        assert "Pulsar 采集失败" in msg

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_nodata(self, mock_get_logger, checker, check_params):
        """NODATA 时应输出 WARNING 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.NODATA,
        }

        checker._alert(*check_params, result)

        mock_log.warning.assert_called_once()
        msg = mock_log.warning.call_args[0][0]
        assert "无消息" in msg

    @patch("src.dqm.checkers.completeness.get_logger")
    def test_alert_error(self, mock_get_logger, checker, check_params):
        """ERROR 时应输出 CRITICAL 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        result = {
            "status": CheckResult.ERROR,
            "error": "DB down",
        }

        checker._alert(*check_params, result)

        mock_log.critical.assert_called_once()
        msg = mock_log.critical.call_args[0][0]
        assert "异常" in msg


# ---------------------------------------------------------------------------
# 5. _prepare 测试
# ---------------------------------------------------------------------------

class TestPrepare:
    """Pulsar 采集与快照写入测试。"""

    @patch("src.dqm.checkers.completeness.PulsarCollector")
    def test_prepare_success(self, mock_collector_cls, checker, mock_mysql_storage, check_params):
        """正常采集流程：创建采集器 → 采集 → 过滤 → 保存快照。"""
        mock_collector = MagicMock()
        mock_collector_cls.return_value = mock_collector

        raw_messages = [
            {"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"},
            {"mst_type": "PLATE_STOCKS", "compn_stock_code": "600000"},
            {"mst_type": "REGION_PLATE_INFO", "stkcode": "BK0002"},
        ]
        mock_collector.collect.return_value = raw_messages

        checker._prepare(*check_params)

        # 验证采集器被创建和关闭
        mock_collector_cls.assert_called_once()
        mock_collector.collect.assert_called_once_with(check_date=check_params[0])
        mock_collector.close.assert_called_once()

        # 验证快照保存被调用（先 DELETE 再 INSERT）
        # 只保留 2 条 M1 相关消息，所以应有 2 次 INSERT
        insert_calls = [
            c for c in mock_mysql_storage.execute_update.call_args_list
            if "INSERT INTO dqm_security_info_snapshot" in c[0][0]
        ]
        assert len(insert_calls) == 2

        # 验证 _pulsar_failed 标记为 False
        assert checker._pulsar_failed is False

    @patch("src.dqm.checkers.completeness.PulsarCollector")
    @patch("src.dqm.checkers.completeness.time")
    def test_prepare_pulsar_retry_and_fail(self, mock_time, mock_collector_cls, checker, check_params):
        """Pulsar 连接重试全部失败后设置 _pulsar_failed=True。"""
        mock_collector = MagicMock()
        mock_collector_cls.return_value = mock_collector
        mock_collector.collect.side_effect = Exception("Connection refused")

        checker._prepare(*check_params)

        assert checker._pulsar_failed is True
        assert "Connection refused" in checker._pulsar_error
        # 验证重试次数 = PULSAR_CONNECT_RETRY (2)
        assert mock_collector.collect.call_count == 2

    @patch("src.dqm.checkers.completeness.PulsarCollector")
    def test_prepare_pulsar_retry_succeed_on_second(self, mock_collector_cls, checker, mock_mysql_storage, check_params):
        """Pulsar 第一次失败，第二次成功。"""
        mock_collector = MagicMock()
        mock_collector_cls.return_value = mock_collector

        mock_collector.collect.side_effect = [
            Exception("Timeout"),
            [{"mst_type": "INDUSTRY_PLATE_INFO", "stkcode": "BK0001"}],
        ]

        checker._prepare(*check_params)

        assert checker._pulsar_failed is False
        assert mock_collector.collect.call_count == 2

    @patch("src.dqm.checkers.completeness.PulsarCollector")
    def test_prepare_empty_messages(self, mock_collector_cls, checker, mock_mysql_storage, check_params):
        """Pulsar 采集到空消息列表。"""
        mock_collector = MagicMock()
        mock_collector_cls.return_value = mock_collector
        mock_collector.collect.return_value = []

        checker._prepare(*check_params)

        # 快照保存时，records 为空不应执行 INSERT
        insert_calls = [
            c for c in mock_mysql_storage.execute_update.call_args_list
            if "INSERT INTO dqm_security_info_snapshot" in c[0][0]
        ]
        assert len(insert_calls) == 0


# ---------------------------------------------------------------------------
# 6. _query_online_keys / _query_snapshot_keys 测试
# ---------------------------------------------------------------------------

class TestQueryKeys:
    """数据库查询方法测试。"""

    def test_query_online_keys(self, checker, mock_mysql_storage, check_params):
        """_query_online_keys 应查询线上表的 DISTINCT key_field。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "BK0001"},
            {"stkcode": "BK0002"},
            {"stkcode": "BK0003"},
        ]

        result = checker._query_online_keys(check_params[0])

        assert result == ["BK0001", "BK0002", "BK0003"]
        sql = mock_mysql_storage.execute_query.call_args[0][0]
        assert "SELECT DISTINCT `stkcode`" in sql
        assert "FROM `gmdb_plate_info`" in sql

    def test_query_online_keys_filters_none(self, checker, mock_mysql_storage, check_params):
        """_query_online_keys 应过滤掉 None/空值。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "BK0001"},
            {"stkcode": None},
            {"stkcode": ""},
            {"stkcode": "BK0002"},
        ]

        result = checker._query_online_keys(check_params[0])

        assert result == ["BK0001", "BK0002"]

    def test_query_online_keys_raises_on_error(self, checker, mock_mysql_storage, check_params):
        """_query_online_keys 查询失败时应抛出异常。"""
        mock_mysql_storage.execute_query.side_effect = Exception("DB Error")

        with pytest.raises(Exception, match="DB Error"):
            checker._query_online_keys(check_params[0])

    def test_query_snapshot_keys(self, checker, mock_mysql_storage, check_params):
        """_query_snapshot_keys 应查询快照表。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "BK0001"},
            {"stkcode": "BK0002"},
        ]

        result = checker._query_snapshot_keys(check_params[0], check_params[2])

        assert result == ["BK0001", "BK0002"]
        sql = mock_mysql_storage.execute_query.call_args[0][0]
        assert "dqm_security_info_snapshot" in sql
        assert "check_date" in sql
        assert "check_round" in sql

    def test_query_snapshot_keys_filters_none(self, checker, mock_mysql_storage, check_params):
        """_query_snapshot_keys 应过滤掉 None/空值。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "BK0001"},
            {"stkcode": None},
        ]

        result = checker._query_snapshot_keys(check_params[0], check_params[2])

        assert result == ["BK0001"]


# ---------------------------------------------------------------------------
# 7. execute 模板方法整体流程测试
# ---------------------------------------------------------------------------

class TestExecute:
    """execute 模板方法集成测试（mock 所有子步骤）。"""

    @patch.object(CompletenessChecker, "_alert")
    @patch.object(CompletenessChecker, "_record")
    @patch.object(CompletenessChecker, "_check")
    @patch.object(CompletenessChecker, "_prepare")
    def test_execute_calls_steps_in_order(
        self, mock_prepare, mock_check, mock_record, mock_alert, checker, check_params
    ):
        """execute 应按 _prepare → _check → _record → _alert 顺序调用。"""
        mock_check.return_value = {
            "status": CheckResult.PASS,
            "missing": [],
            "extra": [],
            "online_count": 5,
            "snapshot_count": 5,
        }

        checker.execute(*check_params)

        mock_prepare.assert_called_once_with(*check_params)
        mock_check.assert_called_once_with(*check_params)
        mock_record.assert_called_once()
        mock_alert.assert_called_once()

    @patch.object(CompletenessChecker, "_alert")
    @patch.object(CompletenessChecker, "_record")
    @patch.object(CompletenessChecker, "_check")
    @patch.object(CompletenessChecker, "_prepare")
    def test_execute_raises_on_prepare_error(
        self, mock_prepare, mock_check, mock_record, mock_alert, checker, check_params
    ):
        """_prepare 抛出异常时，execute 应向上抛出，不调用后续步骤。"""
        mock_prepare.side_effect = Exception("Pulsar down")

        with pytest.raises(Exception, match="Pulsar down"):
            checker.execute(*check_params)

        mock_check.assert_not_called()
        mock_record.assert_not_called()
        mock_alert.assert_not_called()


# ---------------------------------------------------------------------------
# 8. CompletenessChecker 初始化测试
# ---------------------------------------------------------------------------

class TestInit:
    """CompletenessChecker 初始化测试。"""

    def test_init_attributes(self, mock_mysql_storage):
        """验证初始化属性正确设置。"""
        checker = CompletenessChecker(
            monitor_id=MonitorID.M1,
            table="gmdb_plate_info",
            key_field="stkcode",
            mysql_storage=mock_mysql_storage,
        )

        assert checker.monitor_id == MonitorID.M1
        assert checker.dimension == Dimension.COMPLETENESS
        assert checker.table == "gmdb_plate_info"
        assert checker.key_field == "stkcode"
        assert checker._mysql_storage is mock_mysql_storage
