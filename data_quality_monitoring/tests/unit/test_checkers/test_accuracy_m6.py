"""M6 准确性检查器单元测试。

测试 AccuracyChecker 针对 M6 (ads_fin_index_compn_stock_interface_ds 字段约束) 的所有核心逻辑：
1. _validate_field — 单字段校验（非空/正则/类型）
2. _check — 准确性检查主逻辑（PASS/FAIL/采样）
3. _record — 检查结果 + 明细写入
4. _alert — 告警日志输出
5. _prepare — 无操作
6. execute — 模板方法整体流程
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from config.constants import CheckResult, Dimension, MonitorID
from src.dqm.checkers.accuracy import AccuracyChecker, ADS_FIN_INDEX_FIELDS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_mysql_storage():
    """模拟 MySQLStorage。"""
    return MagicMock()


@pytest.fixture
def checker(mock_mysql_storage):
    """构造 M6 AccuracyChecker 实例。"""
    return AccuracyChecker(
        monitor_id=MonitorID.M6,
        table="ads_fin_index_compn_stock_interface_ds",
        key_field="compn_stock_code",
        mysql_storage=mock_mysql_storage,
    )


@pytest.fixture
def check_params():
    """通用检查参数。"""
    return date(2026, 4, 22), datetime(2026, 4, 22, 9, 0, 0), 1


# ---------------------------------------------------------------------------
# 1. _validate_field — 单字段校验
# ---------------------------------------------------------------------------

class TestValidateField:
    """测试 _validate_field 方法的各种校验规则。"""

    def test_required_field_null(self, checker):
        """必填字段为 NULL → NULL_VALUE。"""
        row = {"stkcode": None}
        rule = {"name": "stkcode", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"
        assert error["field_name"] == "stkcode"

    def test_required_field_empty_string(self, checker):
        """必填字段为空字符串 → NULL_VALUE。"""
        row = {"compn_stock_name": ""}
        rule = {"name": "compn_stock_name", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"

    def test_required_field_whitespace(self, checker):
        """必填字段为纯空格 → NULL_VALUE。"""
        row = {"index_name": "   "}
        rule = {"name": "index_name", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"

    def test_optional_field_null_skip(self, checker):
        """非必填字段为 NULL → 跳过，不报错。"""
        row = {"valid_from": None}
        rule = {"name": "valid_from", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_compn_stock_code_required_null(self, checker):
        """compn_stock_code 为 NULL → NULL_VALUE。"""
        row = {"compn_stock_code": None}
        rule = {"name": "compn_stock_code", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"

    def test_send_date_regex_pass_yyyymmdd(self, checker):
        """send_date 格式 YYYYMMDD → 通过。"""
        row = {"send_date": "20260422"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_send_date_regex_pass_dash(self, checker):
        """send_date 格式 YYYY-MM-DD → 通过。"""
        row = {"send_date": "2026-04-22"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_send_date_regex_fail(self, checker):
        """send_date 格式非法 → REGEX_MISMATCH。"""
        row = {"send_date": "22/04/2026"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "REGEX_MISMATCH"

    def test_number_type_valid(self, checker):
        """数值字段合法 → 通过。"""
        row = {"valid_from": 1713369600}
        rule = {"name": "valid_from", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_number_type_invalid_string(self, checker):
        """数值字段为非数值字符串 → TYPE_MISMATCH。"""
        row = {"valid_from": "not_a_number"}
        rule = {"name": "valid_from", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "TYPE_MISMATCH"

    def test_number_type_float_pass(self, checker):
        """数值字段为浮点数 → 通过。"""
        row = {"valid_to": 1713369600.5}
        rule = {"name": "valid_to", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_timestamp_number_valid(self, checker):
        """timestamp 数值字段合法 → 通过。"""
        row = {"timestamp": 1713369600}
        rule = {"name": "timestamp", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_optional_number_null_skip(self, checker):
        """非必填数值字段为 NULL → 跳过。"""
        row = {"valid_to": None}
        rule = {"name": "valid_to", "required": False, "type": "number", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_compn_stock_thscode_optional_null(self, checker):
        """compn_stock_thscode 非必填，为 NULL → 跳过。"""
        row = {"compn_stock_thscode": None}
        rule = {"name": "compn_stock_thscode", "required": False, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None


# ---------------------------------------------------------------------------
# 2. _check — 准确性检查主逻辑
# ---------------------------------------------------------------------------

class TestCheck:
    """测试 _check 方法。"""

    def test_check_all_pass(self, checker, mock_mysql_storage, check_params):
        """所有数据均合法 → PASS。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": "993305", "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total"] == 1
        assert result["errors"] == 0
        assert result["details"] == []

    def test_check_with_null_required_field(self, checker, mock_mysql_storage, check_params):
        """存在 NULL 必填字段 → FAIL + 记录 NULL_VALUE。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": None, "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["errors"] >= 1
        null_errors = [d for d in result["details"] if d["error_type"] == "NULL_VALUE"]
        assert len(null_errors) >= 1

    def test_check_with_regex_error(self, checker, mock_mysql_storage, check_params):
        """send_date 正则不匹配 → FAIL + REGEX_MISMATCH。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": "993305", "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "bad_date",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        regex_errors = [d for d in result["details"] if d["error_type"] == "REGEX_MISMATCH"]
        assert len(regex_errors) >= 1
        assert regex_errors[0]["field_name"] == "send_date"

    def test_check_with_type_error(self, checker, mock_mysql_storage, check_params):
        """数值字段类型错误 → FAIL + TYPE_MISMATCH。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": "993305", "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": "abc", "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        type_errors = [d for d in result["details"] if d["error_type"] == "TYPE_MISMATCH"]
        assert len(type_errors) == 1
        assert type_errors[0]["field_name"] == "valid_from"

    def test_check_multiple_errors_in_one_row(self, checker, mock_mysql_storage, check_params):
        """同一行多个字段异常 → 每个字段都记录。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "", "stkcode": None, "compn_stock_name": "",
             "index_name": None, "send_date": "bad",
             "compn_stock_thscode": None, "valid_from": "abc", "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        # 至少有：compn_stock_code NULL, stkcode NULL, compn_stock_name NULL, index_name NULL, send_date REGEX, valid_from TYPE
        assert result["errors"] >= 6

    def test_check_multiple_rows(self, checker, mock_mysql_storage, check_params):
        """多行数据，部分有错误 → FAIL。"""
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": "993305", "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
            {"compn_stock_code": "000002", "stkcode": "993306", "compn_stock_name": None,
             "index_name": "区域板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["total"] == 2
        assert result["errors"] >= 1

    def test_check_sends_correct_send_date(self, checker, mock_mysql_storage, check_params):
        """验证查询使用了正确的 send_date 参数。"""
        mock_mysql_storage.execute_query.return_value = []
        checker._check(*check_params)
        call_args = mock_mysql_storage.execute_query.call_args
        assert call_args[0][1] == (20260422,)

    def test_check_uses_m6_field_rules(self, checker):
        """M6 checker 使用 ADS_FIN_INDEX_FIELDS 字段规则。"""
        assert checker._field_rules == ADS_FIN_INDEX_FIELDS
        # 确认是 M6 的字段规则（不是 M3 的）
        field_names = [r["name"] for r in checker._field_rules]
        assert "compn_stock_code" in field_names
        assert "index_name" in field_names
        # M3 独有字段不应在 M6 规则中
        assert "std_stkcode" not in field_names
        assert "mst_type" not in field_names


# ---------------------------------------------------------------------------
# 3. _record — 检查结果记录
# ---------------------------------------------------------------------------

class TestRecord:
    """测试 _record 方法。"""

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_record_pass(self, mock_get_logger, checker, check_params):
        """PASS → 写入 dqm_check_result，不写明细。"""
        mock_check_repo = MagicMock()
        mock_detail_repo = MagicMock()
        checker._check_result_repo = mock_check_repo
        checker._accuracy_detail_repo = mock_detail_repo

        result = {
            "status": CheckResult.PASS,
            "send_date": 20260422,
            "total": 100,
            "errors": 0,
            "details": [],
            "sampled": False,
        }
        checker._record(*check_params, result)

        mock_check_repo.upsert.assert_called_once()
        call_kwargs = mock_check_repo.upsert.call_args
        assert call_kwargs[1]["result"] == CheckResult.PASS
        detail = json.loads(call_kwargs[1]["detail"])
        assert detail["total"] == 100
        assert detail["errors"] == 0

        # 无错误 → 不写明细
        mock_detail_repo.save_batch.assert_not_called()

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_record_fail_with_details(self, mock_get_logger, checker, check_params):
        """FAIL → 写入 dqm_check_result + dqm_accuracy_detail。"""
        mock_check_repo = MagicMock()
        mock_detail_repo = MagicMock()
        checker._check_result_repo = mock_check_repo
        checker._accuracy_detail_repo = mock_detail_repo

        error_details = [
            {"record_key": "000001", "field_name": "stkcode", "error_type": "NULL_VALUE", "error_value": "NULL"},
        ]
        result = {
            "status": CheckResult.FAIL,
            "send_date": 20260422,
            "total": 100,
            "errors": 1,
            "details": error_details,
            "sampled": False,
        }
        checker._record(*check_params, result)

        mock_check_repo.upsert.assert_called_once()
        assert mock_check_repo.upsert.call_args[1]["result"] == CheckResult.FAIL

        mock_detail_repo.save_batch.assert_called_once()
        batch_kwargs = mock_detail_repo.save_batch.call_args
        assert batch_kwargs[1]["details"] == error_details


# ---------------------------------------------------------------------------
# 4. _alert — 告警日志输出
# ---------------------------------------------------------------------------

class TestAlert:
    """测试 _alert 方法。"""

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_alert_pass(self, mock_get_logger, checker, check_params):
        """PASS → 输出 INFO 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.PASS,
            "total": 100,
            "errors": 0,
            "details": [],
        }
        checker._alert(*check_params, result)

        mock_log.info.assert_called_once()
        assert "准确性检查通过" in mock_log.info.call_args[0][0]

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_alert_fail(self, mock_get_logger, checker, check_params):
        """FAIL → 输出 ERROR 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.FAIL,
            "total": 100,
            "errors": 3,
            "details": [
                {"record_key": "000001", "field_name": "stkcode", "error_type": "NULL_VALUE", "error_value": "NULL"},
            ],
        }
        checker._alert(*check_params, result)

        mock_log.error.assert_called_once()
        assert "数据异常" in mock_log.error.call_args[0][0]

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_alert_error(self, mock_get_logger, checker, check_params):
        """ERROR → 输出 CRITICAL 日志。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log

        result = {
            "status": CheckResult.ERROR,
            "error": "MySQL connection failed",
        }
        checker._alert(*check_params, result)

        mock_log.critical.assert_called_once()
        assert "准确性检查异常" in mock_log.critical.call_args[0][0]


# ---------------------------------------------------------------------------
# 5. _prepare — 无操作
# ---------------------------------------------------------------------------

class TestPrepare:
    """测试 _prepare 方法（准确性不需要 Pulsar 采集）。"""

    def test_prepare_is_noop(self, checker, check_params):
        """_prepare 应该不做任何事。"""
        result = checker._prepare(*check_params)
        assert result is None


# ---------------------------------------------------------------------------
# 6. execute — 模板方法整体流程
# ---------------------------------------------------------------------------

class TestExecute:
    """测试 execute 模板方法。"""

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_execute_pass_flow(self, mock_get_logger, checker, mock_mysql_storage, check_params):
        """完整 execute 流程：PASS。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": "993305", "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]

        checker.execute(*check_params)

        info_calls = [c for c in mock_log.info.call_args_list]
        assert any("准确性检查通过" in str(c) for c in info_calls)

    @patch("src.dqm.checkers.accuracy.get_logger")
    def test_execute_fail_flow(self, mock_get_logger, checker, mock_mysql_storage, check_params):
        """完整 execute 流程：FAIL。"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_mysql_storage.execute_query.return_value = [
            {"compn_stock_code": "000001", "stkcode": None, "compn_stock_name": "平安银行",
             "index_name": "银行板块", "send_date": "20260422",
             "compn_stock_thscode": None, "valid_from": None, "valid_to": None, "timestamp": None},
        ]

        checker.execute(*check_params)

        error_calls = [c for c in mock_log.error.call_args_list]
        assert any("数据异常" in str(c) for c in error_calls)
