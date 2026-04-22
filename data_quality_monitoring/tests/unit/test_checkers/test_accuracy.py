"""M3 准确性检查器单元测试。

测试 AccuracyChecker 针对 M3 (gmdb_plate_info 字段约束) 的所有核心逻辑：
1. _validate_field — 单字段校验（非空/正则/枚举/类型）
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
from src.dqm.checkers.accuracy import AccuracyChecker, GMDB_PLATE_INFO_FIELDS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_mysql_storage():
    """模拟 MySQLStorage。"""
    return MagicMock()


@pytest.fixture
def checker(mock_mysql_storage):
    """构造 M3 AccuracyChecker 实例。"""
    return AccuracyChecker(
        monitor_id=MonitorID.M3,
        table="gmdb_plate_info",
        key_field="stkcode",
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
        row = {"stkname": ""}
        rule = {"name": "stkname", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"

    def test_required_field_whitespace(self, checker):
        """必填字段为纯空格 → NULL_VALUE。"""
        row = {"stkname": "   "}
        rule = {"name": "stkname", "required": True, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "NULL_VALUE"

    def test_optional_field_null_skip(self, checker):
        """非必填字段为 NULL → 跳过，不报错。"""
        row = {"send_time": None}
        rule = {"name": "send_time", "required": False, "type": "string", "regex": None, "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_regex_match_pass(self, checker):
        """正则匹配通过 → 不报错。"""
        row = {"stkcode": "000001"}
        rule = {"name": "stkcode", "required": True, "type": "string", "regex": r"^[0-9]{6}$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_regex_mismatch(self, checker):
        """正则不匹配 → REGEX_MISMATCH。"""
        row = {"stkcode": "123"}
        rule = {"name": "stkcode", "required": True, "type": "string", "regex": r"^[0-9]{6}$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "REGEX_MISMATCH"
        assert error["error_value"] == "123"

    def test_std_stkcode_regex_pass(self, checker):
        """std_stkcode 正确格式 → 通过。"""
        row = {"std_stkcode": "000001.SH"}
        rule = {"name": "std_stkcode", "required": True, "type": "string",
                "regex": r"^[0-9]{6}\.[A-Z]{2}$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_std_stkcode_regex_fail(self, checker):
        """std_stkcode 格式错误 → REGEX_MISMATCH。"""
        row = {"std_stkcode": "000001sh"}
        rule = {"name": "std_stkcode", "required": True, "type": "string",
                "regex": r"^[0-9]{6}\.[A-Z]{2}$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "REGEX_MISMATCH"

    def test_enum_valid(self, checker):
        """枚举值合法 → 通过。"""
        row = {"mst_type": "INDUSTRY_PLATE_INFO"}
        rule = {"name": "mst_type", "required": True, "type": "string", "regex": None,
                "enum": ["INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"]}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_enum_invalid(self, checker):
        """枚举值不合法 → ENUM_INVALID。"""
        row = {"mst_type": "INVALID_TYPE"}
        rule = {"name": "mst_type", "required": True, "type": "string", "regex": None,
                "enum": ["INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"]}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "ENUM_INVALID"
        assert error["error_value"] == "INVALID_TYPE"

    def test_number_type_valid(self, checker):
        """数值字段合法 → 通过。"""
        row = {"sum": 10}
        rule = {"name": "sum", "required": False, "type": "number", "regex": None,
                "enum": None, "min_value": 0}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_number_type_invalid_string(self, checker):
        """数值字段为非数值字符串 → TYPE_MISMATCH。"""
        row = {"sum": "abc"}
        rule = {"name": "sum", "required": False, "type": "number", "regex": None,
                "enum": None, "min_value": 0}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "TYPE_MISMATCH"

    def test_number_type_negative(self, checker):
        """数值字段 < min_value → TYPE_MISMATCH。"""
        row = {"sum": -5}
        rule = {"name": "sum", "required": False, "type": "number", "regex": None,
                "enum": None, "min_value": 0}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "TYPE_MISMATCH"

    def test_number_type_float_pass(self, checker):
        """数值字段为浮点数且 >= 0 → 通过。"""
        row = {"sum": 3.14}
        rule = {"name": "sum", "required": False, "type": "number", "regex": None,
                "enum": None, "min_value": 0}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_send_date_format_yyyymmdd(self, checker):
        """send_date 格式 YYYYMMDD → 通过。"""
        row = {"send_date": "20260422"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_send_date_format_dash(self, checker):
        """send_date 格式 YYYY-MM-DD → 通过。"""
        row = {"send_date": "2026-04-22"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is None

    def test_send_date_format_invalid(self, checker):
        """send_date 格式非法 → REGEX_MISMATCH。"""
        row = {"send_date": "22/04/2026"}
        rule = {"name": "send_date", "required": True, "type": "string",
                "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$", "enum": None}
        error = checker._validate_field(row, rule, "000001")
        assert error is not None
        assert error["error_type"] == "REGEX_MISMATCH"

    def test_optional_number_null_skip(self, checker):
        """非必填数值字段为 NULL → 跳过。"""
        row = {"sum": None}
        rule = {"name": "sum", "required": False, "type": "number", "regex": None,
                "enum": None, "min_value": 0}
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
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": 90000, "index_type_name": "行业"},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.PASS
        assert result["total"] == 1
        assert result["errors"] == 0
        assert result["details"] == []

    def test_check_with_null_field(self, checker, mock_mysql_storage, check_params):
        """存在 NULL 字段 → FAIL + 记录 NULL_VALUE。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "000001", "stkname": None, "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        assert result["errors"] >= 1
        null_errors = [d for d in result["details"] if d["error_type"] == "NULL_VALUE"]
        assert len(null_errors) >= 1

    def test_check_with_regex_error(self, checker, mock_mysql_storage, check_params):
        """正则不匹配 → FAIL + REGEX_MISMATCH。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "12", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        regex_errors = [d for d in result["details"] if d["error_type"] == "REGEX_MISMATCH"]
        assert len(regex_errors) >= 1
        assert regex_errors[0]["field_name"] == "stkcode"

    def test_check_with_enum_error(self, checker, mock_mysql_storage, check_params):
        """枚举值不合法 → FAIL + ENUM_INVALID。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "000001", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "BAD_TYPE",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        enum_errors = [d for d in result["details"] if d["error_type"] == "ENUM_INVALID"]
        assert len(enum_errors) == 1
        assert enum_errors[0]["field_name"] == "mst_type"

    def test_check_with_type_error(self, checker, mock_mysql_storage, check_params):
        """数值字段类型错误 → FAIL + TYPE_MISMATCH。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "000001", "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": "not_a_number", "index_type_name": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        type_errors = [d for d in result["details"] if d["error_type"] == "TYPE_MISMATCH"]
        assert len(type_errors) == 1
        assert type_errors[0]["field_name"] == "send_time"

    def test_check_multiple_errors_in_one_row(self, checker, mock_mysql_storage, check_params):
        """同一行多个字段异常 → 每个字段都记录。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": None, "stkname": "", "std_stkcode": "bad",
             "zhishubankuaileibie": "行业", "mst_type": "INVALID",
             "send_date": "bad_date", "send_time": None, "index_type_name": None},
        ]
        result = checker._check(*check_params)
        assert result["status"] == CheckResult.FAIL
        # 至少有：stkcode NULL, stkname NULL, std_stkcode REGEX, mst_type ENUM, send_date REGEX
        assert result["errors"] >= 5

    def test_check_multiple_rows(self, checker, mock_mysql_storage, check_params):
        """多行数据，部分有错误 → FAIL。"""
        mock_mysql_storage.execute_query.return_value = [
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
            {"stkcode": "000002", "stkname": None, "std_stkcode": "000002.SZ",
             "zhishubankuaileibie": "区域", "mst_type": "REGION_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
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
            {"record_key": "000001", "field_name": "stkname", "error_type": "NULL_VALUE", "error_value": "NULL"},
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
        assert call_kwargs_result(mock_check_repo) == CheckResult.FAIL

        mock_detail_repo.save_batch.assert_called_once()
        batch_kwargs = mock_detail_repo.save_batch.call_args
        assert batch_kwargs[1]["details"] == error_details


def call_kwargs_result(mock_repo):
    """辅助：获取 upsert 调用中的 result 参数。"""
    return mock_repo.upsert.call_args[1]["result"]


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
                {"record_key": "000001", "field_name": "stkname", "error_type": "NULL_VALUE", "error_value": "NULL"},
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
            {"stkcode": "000001", "stkname": "平安银行", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
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
            {"stkcode": None, "stkname": "测试", "std_stkcode": "000001.SH",
             "zhishubankuaileibie": "行业", "mst_type": "INDUSTRY_PLATE_INFO",
             "send_date": "20260422", "send_time": None, "index_type_name": None},
        ]

        checker.execute(*check_params)

        error_calls = [c for c in mock_log.error.call_args_list]
        assert any("数据异常" in str(c) for c in error_calls)
