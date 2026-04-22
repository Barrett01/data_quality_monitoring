"""及时性检查器：验证线上表是否存在当天数据。"""

from __future__ import annotations

import json
from datetime import date, datetime

from config.constants import (
    CheckResult,
    Dimension,
    MonitorID,
)
from config.logger_config import get_logger
from src.dqm.alerts.formatter import AlertFormatter
from src.dqm.checkers.base import BaseChecker
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import CheckResultRepository


class TimelinessChecker(BaseChecker):
    """及时性检查器

    M2: gmdb_plate_info 的 send_date 是否存在当天数据
    M5: ads_fin_index_compn_stock_interface_ds 的 send_date 是否存在当天数据

    注意：send_date 在数据库中为 int 类型，格式为 YYYYMMDD（如 20260421）
    """

    def __init__(
        self,
        monitor_id: str,
        table: str,
        mysql_storage: MySQLStorage,
    ):
        super().__init__(monitor_id, Dimension.TIMELINESS)
        self.table = table
        self._mysql_storage = mysql_storage
        self._check_result_repo = CheckResultRepository(mysql_storage)

    @staticmethod
    def _date_to_send_date(check_date: date) -> int:
        """将 date 转换为数据库中的 send_date 格式（YYYYMMDD 整数）。"""
        return int(check_date.strftime("%Y%m%d"))

    def _prepare(self, check_date: date, check_time: datetime, check_round: int):
        """及时性检查无需 Pulsar 采集，直接查线上表即可。"""
        pass

    def _check(self, check_date: date, check_time: datetime, check_round: int) -> dict:
        """查询线上表是否存在 send_date = check_date 的数据。"""
        log = get_logger(self.monitor_id, self.dimension)
        send_date_val = self._date_to_send_date(check_date)

        # M2: 按 mst_type 分组统计当天各类型数据量
        # M5: 只查 mst_type='PLATE_STOCKS' 的当天数据
        if self.monitor_id == MonitorID.M2:
            results = self._query_by_mst_type(send_date_val)
        elif self.monitor_id == MonitorID.M5:
            results = self._query_by_mst_type(send_date_val, mst_type="PLATE_STOCKS")
        else:
            results = self._query_count(send_date_val)

        log.info(f"线上表查询完成 | table={self.table}, send_date={send_date_val}, results={results}")

        # 判断是否存在当天数据
        total_count = sum(r["count"] for r in results)
        if total_count == 0:
            return {
                "status": CheckResult.FAIL,
                "check_date": str(check_date),
                "send_date": send_date_val,
                "total_count": 0,
                "details": [],
            }

        return {
            "status": CheckResult.PASS,
            "check_date": str(check_date),
            "send_date": send_date_val,
            "total_count": total_count,
            "details": results,
        }

    def _query_by_mst_type(self, send_date: int, mst_type: str | None = None) -> list[dict]:
        """按 mst_type 分组查询当天数据量。

        Args:
            send_date: 发送日期（YYYYMMDD 整数格式）
            mst_type: 消息类型过滤，为 None 时查所有类型并分组
        """
        if mst_type:
            sql = (
                f"SELECT mst_type, COUNT(*) AS cnt FROM `{self.table}` "
                f"WHERE send_date = %s AND mst_type = %s "
                f"GROUP BY mst_type"
            )
            rows = self._mysql_storage.execute_query(sql, (send_date, mst_type))
        else:
            sql = (
                f"SELECT mst_type, COUNT(*) AS cnt FROM `{self.table}` "
                f"WHERE send_date = %s "
                f"GROUP BY mst_type"
            )
            rows = self._mysql_storage.execute_query(sql, (send_date,))

        results = []
        for row in rows:
            results.append({
                "mst_type": row.get("mst_type", ""),
                "count": row.get("cnt", 0),
            })
        return results

    def _query_count(self, send_date: int) -> list[dict]:
        """简单查询当天数据总量。"""
        sql = f"SELECT COUNT(*) AS cnt FROM `{self.table}` WHERE send_date = %s"
        rows = self._mysql_storage.execute_query(sql, (send_date,))
        cnt = rows[0].get("cnt", 0) if rows else 0
        return [{"mst_type": "", "count": cnt}]

    def _record(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """将检查结果写入 dqm_check_result。"""
        detail = json.dumps(
            {
                "check_date": result["check_date"],
                "send_date": result["send_date"],
                "total_count": result["total_count"],
                "details": result.get("details", []),
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

    def _alert(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """根据检查结果输出告警日志。"""
        log = get_logger(self.monitor_id, self.dimension)
        status = result["status"]

        if status == CheckResult.PASS:
            msg = AlertFormatter.format_timeliness_pass(
                self.monitor_id, self.table, result["total_count"], result["check_date"]
            )
            log.info(msg)

        elif status == CheckResult.FAIL:
            msg = AlertFormatter.format_timeliness_fail(
                self.monitor_id, self.table, result["check_date"]
            )
            log.error(msg)

        elif status == CheckResult.ERROR:
            log.critical(f"及时性检查异常 | error={result.get('error', 'unknown')}")
