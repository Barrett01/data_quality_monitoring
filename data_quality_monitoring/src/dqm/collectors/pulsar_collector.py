"""Pulsar 消息采集器：监听实时消息并写入快照。

采集策略：
- 从 Earliest 位置订阅，采集全量历史消息，确保不遗漏
- 持续采集直到空闲超时（连续 IDLE_TIMEOUT_SECONDS 无新消息）
- 仅保留当天消息（通过 send_date 字段过滤）
采集结果写入快照表（INSERT IGNORE），供 M1/M4 共同使用。
同一天已有快照时由 CompletenessChecker._prepare 跳过采集，不在本层判断。
"""

from __future__ import annotations

import json
import time

import pulsar

from config.logger_config import get_logger
from config.settings import (
    PULSAR_SERVICE_URL,
    PULSAR_TOPIC,
    PULSAR_SUBSCRIPTION_NAME,
    PULSAR_COLLECT_WINDOW_SECONDS,
    PULSAR_RECEIVE_TIMEOUT_MS,
)

logger = get_logger("SYSTEM", "Pulsar")

# 连续无消息的超时阈值（秒）：超过此时间没有收到新消息则认为当天消息已全部接收
IDLE_TIMEOUT_SECONDS = 10


class PulsarCollector:
    """Pulsar 消息采集器

    从 Earliest 开始采集全量消息，过滤保留当天消息。
    持续接收直到空闲超时无新消息为止。
    """

    def __init__(self):
        self._client: pulsar.Client | None = None

    def _ensure_connection(self):
        if self._client is None:
            self._client = pulsar.Client(PULSAR_SERVICE_URL)
            logger.info("Pulsar 连接已建立")

    def collect(self, check_date, topic: str = "", subscription: str = "", **kwargs) -> list[dict]:
        """采集当天全量 Pulsar 消息。

        从 Earliest 位置订阅，持续接收直到连续 IDLE_TIMEOUT_SECONDS
        没有新消息为止，确保收集当天所有已推送消息。

        Args:
            check_date: 检查日期，用于过滤当天消息
            topic: Pulsar topic，为空则使用配置默认值
            subscription: 订阅名称，为空则使用配置默认值
        """
        self._ensure_connection()
        topic = topic or PULSAR_TOPIC
        subscription = subscription or PULSAR_SUBSCRIPTION_NAME

        logger.info("Pulsar 订阅位置: Earliest(全量)")

        consumer = self._client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
        )

        messages = []
        last_receive_time = time.time()
        max_collect_seconds = PULSAR_COLLECT_WINDOW_SECONDS  # 最大采集时长上限
        start_time = time.time()

        try:
            while True:
                elapsed = time.time() - start_time
                idle = time.time() - last_receive_time

                # 超过最大采集时长或空闲超时，停止采集
                if elapsed >= max_collect_seconds:
                    logger.info(f"Pulsar 采集达到最大时长 {max_collect_seconds}s，停止")
                    break
                if idle >= IDLE_TIMEOUT_SECONDS and len(messages) > 0:
                    logger.info(f"Pulsar 采集空闲 {IDLE_TIMEOUT_SECONDS}s 无新消息，停止")
                    break

                try:
                    msg = consumer.receive(timeout_millis=PULSAR_RECEIVE_TIMEOUT_MS)
                    try:
                        data = json.loads(msg.data().decode("utf-8"))
                        # 仅保留当天的消息（基于 send_date 字段判断）
                        msg_send_date = data.get("send_date")
                        if msg_send_date is not None:
                            send_date_str = str(msg_send_date).replace("-", "")
                            today_str = check_date.strftime("%Y%m%d")
                            if send_date_str != today_str:
                                consumer.acknowledge(msg)
                                continue
                        messages.append(data)
                        last_receive_time = time.time()
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Pulsar 消息解码失败，跳过 | error={e}")
                    finally:
                        consumer.acknowledge(msg)
                except pulsar.Timeout:
                    # 首次无消息时等待更久，给消息到达的时间
                    if len(messages) == 0 and idle < max_collect_seconds:
                        continue
                    # 已有消息但超时，检查是否达到空闲阈值
                    if len(messages) > 0 and idle >= IDLE_TIMEOUT_SECONDS:
                        logger.info(f"Pulsar 采集空闲超时，停止")
                        break
                    continue
        finally:
            try:
                consumer.close()
            except Exception as e:
                logger.warning(f"Pulsar Consumer 关闭异常 | error={e}")

        logger.info(f"Pulsar 采集完成: received={len(messages)}")
        return messages

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Pulsar 连接已关闭")
