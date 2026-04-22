"""告警格式化器：将检查结果格式化为标准日志消息。"""

import json


class AlertFormatter:
    """告警格式化器"""

    @staticmethod
    def format_check_start(monitor_id: str, dimension: str, check_date: str, check_round: int) -> str:
        return f"开始{dimension}检查 | check_date={check_date}, check_round={check_round}"

    @staticmethod
    def format_completeness_pass(monitor_id: str, table: str, online: int, snapshot: int) -> str:
        return f"{table} 核心代码完全一致 | online={online}, snapshot={snapshot}, missing=0, extra=0"

    @staticmethod
    def format_completeness_fail(monitor_id: str, table: str, online: int, snapshot: int, missing: list, extra: list) -> str:
        return (
            f"{table} 核心代码不一致 | online={online}, snapshot={snapshot}, "
            f"missing={json.dumps(missing, ensure_ascii=False)}, extra={json.dumps(extra, ensure_ascii=False)}"
        )

    @staticmethod
    def format_completeness_fail_grouped(
        monitor_id: str, table: str, online: int, snapshot: int, group_details: list
    ) -> str:
        """M4 分组比对失败时的格式化。"""
        summary = []
        for g in group_details[:5]:
            summary.append(
                f"{g['group_key']}: missing={len(g['missing'])}, extra={len(g['extra'])}"
            )
        return (
            f"{table} 成分股代码不一致 | online={online}, snapshot={snapshot}, "
            f"diff_groups={len(group_details)}, details=[{'; '.join(summary)}]"
        )

    @staticmethod
    def format_timeliness_pass(monitor_id: str, table: str, count: int, send_date: str) -> str:
        return f"{table} 当天数据已到达 | count={count}, send_date={send_date}"

    @staticmethod
    def format_timeliness_fail(monitor_id: str, table: str, send_date: str) -> str:
        return f"{table} 无当天数据 | send_date={send_date}"

    @staticmethod
    def format_accuracy_pass(monitor_id: str, table: str, total: int) -> str:
        return f"{table} 准确性检查通过 | total={total}, errors=0"

    @staticmethod
    def format_accuracy_fail(monitor_id: str, table: str, total: int, errors: int, details: list) -> str:
        return (
            f"{table} 数据异常 | total={total}, errors={errors}, "
            f"details={json.dumps(details[:5], ensure_ascii=False)}"
        )

    @staticmethod
    def format_system_error(component: str, error: str) -> str:
        return f"{component}异常 | error={error}"
