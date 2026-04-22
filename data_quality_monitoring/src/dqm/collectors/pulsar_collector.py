"""Pulsar 消息采集器：监听实时消息并写入快照。"""

from __future__ import annotations

import json
import time

import pulsar

from config.logger_config import get_logger
from config.settings import PULSAR_SERVICE_URL, PULSAR_TOPIC, PULSAR_SUBSCRIPTION_NAME, PULSAR_COLLECT_WINDOW_SECONDS, PULSAR_RECEIVE_TIMEOUT_MS
logger = get_logger("SYSTEM", "Pulsar")


class PulsarCollector:
    """Pulsar 消息采集器"""

    def __init__(self):
        self._client: pulsar.Client | None = None

    def _ensure_connection(self):
        if self._client is None:
            self._client = pulsar.Client(PULSAR_SERVICE_URL)
            logger.info("Pulsar 连接已建立")

    def collect(self, check_date, topic: str = "", subscription: str = "", window_seconds: int = 0, **kwargs) -> list[dict]:
        """在时间窗口内采集 Pulsar 消息。"""
        self._ensure_connection()
        topic = topic or PULSAR_TOPIC
        subscription = subscription or PULSAR_SUBSCRIPTION_NAME
        window_seconds = window_seconds or PULSAR_COLLECT_WINDOW_SECONDS

        consumer = self._client.subscribe(
            topic, subscription, consumer_type=pulsar.ConsumerType.Exclusive
        )

        messages = []
        start_time = time.time()
        try:
            while time.time() - start_time < window_seconds:
                try:
                    msg = consumer.receive(timeout_millis=PULSAR_RECEIVE_TIMEOUT_MS)
                    data = json.loads(msg.data().decode("utf-8"))
                    messages.append(data)
                    consumer.acknowledge(msg)
                except pulsar.Timeout:
                    continue
        finally:
            consumer.close()

        logger.info(f"Pulsar 采集完成: received={len(messages)}, window={window_seconds}s")
        return messages

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Pulsar 连接已关闭")
