"""准确性检查器：校验字段非空和类型约束。

M3: gmdb_plate_info 字段约束校验
M6: ads_fin_index_compn_stock_interface_ds 字段约束校验

校验维度：
1. 非空检查：必填字段为 NULL 或空字符串 → NULL_VALUE
2. 正则检查：字段值不匹配预定义正则 → REGEX_MISMATCH
3. 枚举检查：字段值不在预定义枚举集合中 → ENUM_INVALID
4. 类型检查：数值字段非数值或 < 0 → TYPE_MISMATCH
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime

from config.constants import (
    CheckResult,
    Dimension,
    MonitorID,
    SAMPLE_RATIO,
    SAMPLE_THRESHOLD,
)
from config.logger_config import get_logger
from src.dqm.alerts.formatter import AlertFormatter
from src.dqm.checkers.base import BaseChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import AccuracyDetailRepository, CheckResultRepository


# ---------------------------------------------------------------------------
# 字段约束定义
# ---------------------------------------------------------------------------

# M3: gmdb_plate_info 字段约束（基于真实表结构）
GMDB_PLATE_INFO_FIELDS = [
    {
        "name": "stkcode",
        "required": True,
        "type": "string",
        "regex": r"^[0-9]{6}$",
        "enum": None,
    },
    {
        "name": "stkname",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "std_stkcode",
        "required": True,
        "type": "string",
        "regex": r"^[0-9]{6}\.[A-Z]{2}$",
        "enum": None,
    },
    {
        "name": "zhishubankuaileibie",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "mst_type",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": ["INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"],
    },
    {
        "name": "send_date",
        "required": True,
        "type": "string",
        "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$",
        "enum": None,
    },
    {
        "name": "send_time",
        "required": False,
        "type": "number",
        "regex": None,
        "enum": None,
    },
    {
        "name": "index_type_name",
        "required": False,
        "type": "string",
        "regex": None,
        "enum": None,
    },
]

# M6: ads_fin_index_compn_stock_interface_ds 字段约束
ADS_FIN_INDEX_FIELDS = [
    {
        "name": "stkcode",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "compn_stock_code",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "compn_stock_name",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "index_name",
        "required": True,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "send_date",
        "required": True,
        "type": "string",
        "regex": r"^(\d{4}-\d{2}-\d{2}|\d{8})$",
        "enum": None,
    },
    {
        "name": "compn_stock_thscode",
        "required": False,
        "type": "string",
        "regex": None,
        "enum": None,
    },
    {
        "name": "valid_from",
        "required": False,
        "type": "number",
        "regex": None,
        "enum": None,
    },
    {
        "name": "valid_to",
        "required": False,
        "type": "number",
        "regex": None,
        "enum": None,
    },
    {
        "name": "timestamp",
        "required": False,
        "type": "number",
        "regex": None,
        "enum": None,
    },
]


class AccuracyChecker(BaseChecker):
    """准确性检查器

    M3: gmdb_plate_info 字段非空 + 类型校验
    M6: ads_fin_index_compn_stock_interface_ds 字段非空 + 类型校验
    """

    def __init__(
        self,
        monitor_id: str,
        table: str,
        key_field: str,
        mysql_storage: MySQLStorage,
    ):
        super().__init__(monitor_id, Dimension.ACCURACY)
        self.table = table
        self.key_field = key_field
        self._mysql_storage = mysql_storage
        self._check_result_repo = CheckResultRepository(mysql_storage)
        self._accuracy_detail_repo = AccuracyDetailRepository(mysql_storage)

        # 根据监控项选择字段约束
        if monitor_id == MonitorID.M3:
            self._field_rules = GMDB_PLATE_INFO_FIELDS
        elif monitor_id == MonitorID.M6:
            self._field_rules = ADS_FIN_INDEX_FIELDS
        else:
            self._field_rules = []

    def _prepare(self, check_date: date, check_time: datetime, check_round: int):
        """准确性检查无需 Pulsar 采集，直接查线上表即可。"""
        pass

    def _check(self, check_date: date, check_time: datetime, check_round: int) -> dict:
        """查询线上表数据并逐行逐字段校验。"""
        log = get_logger(self.monitor_id, self.dimension)
        send_date_val = self._date_to_send_date(check_date)

        # 查询线上表当天数据
        rows = self._query_table_data(send_date_val)
        total = len(rows)
        log.info(f"线上表查询完成 | table={self.table}, send_date={send_date_val}, total={total}")

        # 数据量过大时采样
        sampled = False
        if total > SAMPLE_THRESHOLD:
            rows = rows[: int(total * SAMPLE_RATIO)]
            sampled = True
            log.info(f"数据量超阈值，采样检查 | total={total}, sample={len(rows)}")

        # 逐行逐字段校验
        error_details = []
        for row in rows:
            record_key = str(row.get(self.key_field, ""))
            for rule in self._field_rules:
                error = self._validate_field(row, rule, record_key)
                if error:
                    error_details.append(error)

        error_count = len(error_details)
        status = CheckResult.FAIL if error_count > 0 else CheckResult.PASS

        return {
            "status": status,
            "total": total,
            "errors": error_count,
            "details": error_details,
            "sampled": sampled,
            "send_date": send_date_val,
        }

    def _query_table_data(self, send_date: int) -> list[dict]:
        """查询线上表中 send_date = check_date 的数据。"""
        field_names = ", ".join(f"`{r['name']}`" for r in self._field_rules)
        sql = f"SELECT `{self.key_field}`, {field_names} FROM `{self.table}` WHERE send_date = %s"
        try:
            return self._mysql_storage.execute_query(sql, (send_date,))
        except Exception as e:
            log = get_logger(self.monitor_id, self.dimension)
            log.critical(f"MySQL 查询线上表失败 | table={self.table}, error={e}")
            raise

    @staticmethod
    def _date_to_send_date(check_date: date) -> int:
        """将 date 转换为数据库中的 send_date 格式（YYYYMMDD 整数）。"""
        return int(check_date.strftime("%Y%m%d"))

    def _validate_field(self, row: dict, rule: dict, record_key: str) -> dict | None:
        """校验单个字段，返回错误详情或 None。"""
        field_name = rule["name"]
        value = row.get(field_name)

        # 非空检查
        if rule.get("required", False):
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return {
                    "record_key": record_key,
                    "field_name": field_name,
                    "error_type": "NULL_VALUE",
                    "error_value": str(value) if value is not None else "NULL",
                }

        # 非必填且值为空，跳过后续校验
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None

        str_value = str(value)

        # 正则检查
        regex = rule.get("regex")
        if regex and not re.match(regex, str_value):
            return {
                "record_key": record_key,
                "field_name": field_name,
                "error_type": "REGEX_MISMATCH",
                "error_value": str_value,
            }

        # 枚举检查
        enum_values = rule.get("enum")
        if enum_values and str_value not in enum_values:
            return {
                "record_key": record_key,
                "field_name": field_name,
                "error_type": "ENUM_INVALID",
                "error_value": str_value,
            }

        # 类型检查：数值字段
        if rule.get("type") == "number":
            try:
                num_value = float(str_value)
                min_value = rule.get("min_value")
                if min_value is not None and num_value < min_value:
                    return {
                        "record_key": record_key,
                        "field_name": field_name,
                        "error_type": "TYPE_MISMATCH",
                        "error_value": str_value,
                    }
            except (ValueError, TypeError):
                return {
                    "record_key": record_key,
                    "field_name": field_name,
                    "error_type": "TYPE_MISMATCH",
                    "error_value": str_value,
                }

        return None

    def _record(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """将检查结果写入 dqm_check_result 和 dqm_accuracy_detail。"""
        # 写入检查结果
        detail = json.dumps(
            {
                "send_date": result["send_date"],
                "total": result["total"],
                "errors": result["errors"],
                "sampled": result.get("sampled", False),
                "details": result["details"][:50],  # 最多保留 50 条
            },
            ensure_ascii=False,
        )
        self._check_result_repo.upsert(
            check_date=check_date,
            check_round=check_round,
            monitor_id=self.monitor_id,
            dimension=self.dimension,
            result=result["status"],
            detail=detail,
        )

        # 写入准确性明细
        if result["details"]:
            self._accuracy_detail_repo.save_batch(
                check_date=check_date,
                check_round=check_round,
                monitor_id=self.monitor_id,
                details=result["details"],
            )

        # 清理过期明细数据（保留 15 天）
        try:
            self._accuracy_detail_repo.cleanup(retention_days=15)
        except Exception as e:
            log = get_logger(self.monitor_id, self.dimension)
            log.warning(f"清理过期明细数据失败 | error={e}")

    def _record_error(self, check_date: date, check_time: datetime, check_round: int, error_msg: str):
        """异常时记录 ERROR 状态到 dqm_check_result。"""
        import json as _json
        detail = _json.dumps({"error": error_msg}, ensure_ascii=False)
        self._check_result_repo.upsert(
            check_date=check_date,
            check_round=check_round,
            monitor_id=self.monitor_id,
            dimension=self.dimension,
            result=CheckResult.ERROR,
            detail=detail,
        )

    def _alert(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """根据检查结果输出告警日志。"""
        log = get_logger(self.monitor_id, self.dimension)
        status = result["status"]

        if status == CheckResult.PASS:
            msg = AlertFormatter.format_accuracy_pass(
                self.monitor_id, self.table, result["total"]
            )
            log.info(msg)

        elif status == CheckResult.FAIL:
            msg = AlertFormatter.format_accuracy_fail(
                self.monitor_id,
                self.table,
                result["total"],
                result["errors"],
                result["details"],
            )
            log.error(msg)

        elif status == CheckResult.ERROR:
            log.critical(f"准确性检查异常 | error={result.get('error', 'unknown')}")
