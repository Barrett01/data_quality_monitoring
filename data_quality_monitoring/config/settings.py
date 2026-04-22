"""全局配置：从 .env 文件加载扁平常量。"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _int_env(key: str, default: int = 0) -> int:
    return int(os.getenv(key, str(default)))


# ── MySQL ──────────────────────────────────────────────
MYSQL_HOST = _env("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = _int_env("MYSQL_PORT", 3306)
MYSQL_USER = _env("MYSQL_USER", "root")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "your_password")
MYSQL_DATABASE = _env("MYSQL_DATABASE", "segger_db")
MYSQL_CHARSET = _env("MYSQL_CHARSET", "utf8mb4")

# ── MongoDB ────────────────────────────────────────────
MONGO_HOST = _env("MONGO_HOST", "127.0.0.1")
MONGO_PORT = _int_env("MONGO_PORT", 27017)
MONGO_USER = _env("MONGO_USER", "")
MONGO_PASSWORD = _env("MONGO_PASSWORD", "")
MONGO_DATABASE = _env("MONGO_DATABASE", "gmdb")
MONGO_AUTH_SOURCE = _env("MONGO_AUTH_SOURCE", "admin")


def get_mongo_uri() -> str:
    """拼接 MongoDB URI。"""
    if MONGO_USER and MONGO_PASSWORD:
        return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}"
    return f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}"


MONGO_URI = get_mongo_uri()

# ── Pulsar ─────────────────────────────────────────────
PULSAR_SERVICE_URL = _env("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650")
PULSAR_TOPIC = _env("PULSAR_TOPIC", "persistent://public/default/security_info")
PULSAR_SUBSCRIPTION_NAME = _env("PULSAR_SUBSCRIPTION_NAME", "data-quality-monitor-sub")
PULSAR_RECEIVE_TIMEOUT_MS = _int_env("PULSAR_RECEIVE_TIMEOUT_MS", 5000)
PULSAR_COLLECT_WINDOW_SECONDS = _int_env("PULSAR_COLLECT_WINDOW_SECONDS", 30)

# ── 日志 ───────────────────────────────────────────────
LOG_LEVEL = _env("LOG_LEVEL", "INFO")
LOG_DIR = _env("LOG_DIR", "logs")

# ── 监控 ───────────────────────────────────────────────
CHECK_TIMEOUT_SECONDS = _int_env("CHECK_TIMEOUT_SECONDS", 60)
SNAPSHOT_RETENTION_DAYS = _int_env("SNAPSHOT_RETENTION_DAYS", 15)
CHECK_RESULT_RETENTION_DAYS = _int_env("CHECK_RESULT_RETENTION_DAYS", 90)
ACCURACY_DETAIL_RETENTION_DAYS = _int_env("ACCURACY_DETAIL_RETENTION_DAYS", 15)
