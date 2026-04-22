"""完整性检查器：比对线上表与临时快照的核心代码集合。"""

from __future__ import annotations

import json
import time
from datetime import date, datetime

from config.constants import (
    CheckResult,
    Dimension,
    MonitorID,
    PULSAR_CONNECT_RETRY,
    PULSAR_CONNECT_RETRY_INTERVAL,
)
from config.logger_config import get_logger
from src.dqm.alerts.formatter import AlertFormatter
from src.dqm.checkers.base import BaseChecker
from src.dqm.collectors.pulsar_collector import PulsarCollector
from src.dqm.storage.mysql_storage import MySQLStorage
from src.dqm.storage.repository import CheckResultRepository, SnapshotRepository


class CompletenessChecker(BaseChecker):
    """完整性检查器

    M1: gmdb_plate_info 的 stkcode vs Pulsar 快照的 stkcode
    M4: ads_fin_index_compn_stock_interface_ds 的 compn_stock_code vs Pulsar 快照的 compn_stock_code
    """

    def __init__(
        self,
        monitor_id: str,
        table: str,
        key_field: str,
        mysql_storage: MySQLStorage,
    ):
        super().__init__(monitor_id, Dimension.COMPLETENESS)
        self.table = table
        self.key_field = key_field
        self._mysql_storage = mysql_storage
        self._check_result_repo = CheckResultRepository(mysql_storage)
        self._snapshot_repo = SnapshotRepository(mysql_storage)

    def _prepare(self, check_date: date, check_time: datetime, check_round: int):
        """采集 Pulsar 快照数据并写入 MySQL 临时表。"""
        log = get_logger(self.monitor_id, self.dimension)
        log.info(f"开始采集 Pulsar 快照 | check_date={check_date}, check_round={check_round}")

        pulsar_collector = PulsarCollector()
        messages = []
        try:
            # 带重试的 Pulsar 连接
            for attempt in range(1, PULSAR_CONNECT_RETRY + 1):
                try:
                    messages = pulsar_collector.collect(check_date=check_date)
                    break
                except Exception as e:
                    log.warning(f"Pulsar 连接失败(第{attempt}次): {e}")
                    if attempt < PULSAR_CONNECT_RETRY:
                        time.sleep(PULSAR_CONNECT_RETRY_INTERVAL)
                    else:
                        raise
        except Exception as e:
            log.critical(f"Pulsar 连接失败，跳过完整性比对 | error={e}")
            self._pulsar_failed = True
            self._pulsar_error = str(e)
            return
        finally:
            pulsar_collector.close()

        # 过滤并提取关键字段
        filtered = self._filter_messages(messages)
        log.info(
            f"Pulsar 快照采集完成 | received={len(messages)}, filtered={len(filtered)}"
        )

        # 写入临时快照表
        self._snapshot_repo.save(check_date, check_round, filtered)
        self._pulsar_failed = False
        self._pulsar_error = ""

    def _filter_messages(self, messages: list[dict]) -> list[dict]:
        """根据监控项过滤消息，仅保留相关类型。"""
        filtered = []
        for msg in messages:
            mst_type = msg.get("mst_type", "")
            if self.monitor_id == MonitorID.M1:
                if mst_type in (
                    "INDUSTRY_PLATE_INFO",
                    "REGION_PLATE_INFO",
                    "HOTIDEA_PLATE_INFO",
                ):
                    filtered.append(msg)
            elif self.monitor_id == MonitorID.M4:
                if mst_type == "PLATE_STOCKS":
                    filtered.append(msg)
            else:
                filtered.append(msg)
        return filtered

    def _check(self, check_date: date, check_time: datetime, check_round: int) -> dict:
        """比对线上表与临时快照的核心代码集合。"""
        log = get_logger(self.monitor_id, self.dimension)

        # 如果 Pulsar 采集失败，返回 SKIP
        if getattr(self, "_pulsar_failed", False):
            return {
                "status": CheckResult.SKIP,
                "missing": [],
                "extra": [],
                "online_count": 0,
                "snapshot_count": 0,
                "error": self._pulsar_error,
            }

        # 查询线上表的 key_field 集合
        online_keys = self._query_online_keys(check_date)
        log.info(f"线上表查询完成 | table={self.table}, count={len(online_keys)}")

        # 查询临时快照表的 key_field 集合
        snapshot_keys = self._query_snapshot_keys(check_date, check_round)
        log.info(f"快照表查询完成 | count={len(snapshot_keys)}")

        # 集合比对
        online_set = set(online_keys)
        snapshot_set = set(snapshot_keys)

        missing = sorted(snapshot_set - online_set)  # 快照有但线上无 → 数据遗漏
        extra = sorted(online_set - snapshot_set)  # 线上有但快照无 → 数据多余

        if not missing and not extra:
            status = CheckResult.PASS
        else:
            status = CheckResult.FAIL

        # 如果快照为空，标记为 NODATA
        if len(snapshot_set) == 0:
            status = CheckResult.NODATA

        return {
            "status": status,
            "missing": missing,
            "extra": extra,
            "online_count": len(online_set),
            "snapshot_count": len(snapshot_set),
        }

    def _query_online_keys(self, check_date: date) -> list[str]:
        """从 MySQL 线上表查询 key_field 集合。"""
        sql = f"SELECT DISTINCT `{self.key_field}` FROM `{self.table}`"
        try:
            rows = self._mysql_storage.execute_query(sql)
            return [row[self.key_field] for row in rows if row.get(self.key_field)]
        except Exception as e:
            log = get_logger(self.monitor_id, self.dimension)
            log.critical(f"MySQL 查询线上表失败 | table={self.table}, error={e}")
            raise

    def _query_snapshot_keys(self, check_date: date, check_round: int) -> list[str]:
        """从临时快照表查询 key_field 集合。"""
        sql = (
            f"SELECT DISTINCT `{self.key_field}` FROM `dqm_security_info_snapshot` "
            f"WHERE check_date = %s AND check_round = %s"
        )
        rows = self._mysql_storage.execute_query(sql, (check_date, check_round))
        return [row[self.key_field] for row in rows if row.get(self.key_field)]

    def _record(self, check_date: date, check_time: datetime, check_round: int, result: dict):
        """将检查结果写入 dqm_check_result。"""
        detail = json.dumps(
            {
                "missing": result["missing"][:50],
                "extra": result["extra"][:50],
                "online_count": result["online_count"],
                "snapshot_count": result["snapshot_count"],
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
            msg = AlertFormatter.format_completeness_pass(
                self.monitor_id, self.table, result["online_count"], result["snapshot_count"]
            )
            log.info(msg)

        elif status == CheckResult.FAIL:
            msg = AlertFormatter.format_completeness_fail(
                self.monitor_id,
                self.table,
                result["online_count"],
                result["snapshot_count"],
                result["missing"],
                result["extra"],
            )
            log.error(msg)

        elif status == CheckResult.SKIP:
            log.warning(f"Pulsar 采集失败，跳过完整性比对 | error={result.get('error', '')}")

        elif status == CheckResult.NODATA:
            log.warning(f"Pulsar 采集窗口内无消息，无法比对 | table={self.table}")

        elif status == CheckResult.ERROR:
            log.critical(f"完整性检查异常 | error={result.get('error', 'unknown')}")
