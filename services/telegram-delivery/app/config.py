from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "telegram-delivery")
    enabled: bool = _bool_env("TELEGRAM_DELIVERY_ENABLED", False)
    api_base_url: str = os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org")
    timeout_seconds: float = float(os.getenv("TELEGRAM_TIMEOUT_SECONDS", "10"))
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_rules_url: str = os.getenv("REDIS_RULES_URL", "redis://redis-hot-state:6379/1")
    promotion_rules_key_prefix: str = os.getenv("PROMOTION_RULES_KEY_PREFIX", "hunger:promotion-rules:location")
    promotion_rules_revision_key_prefix: str = os.getenv(
        "PROMOTION_RULES_REVISION_KEY_PREFIX", "hunger:promotion-rules:location"
    )
    offers_stream: str = os.getenv("OFFERS_STREAM", "stream:offers")
    consumer_group: str = os.getenv("TELEGRAM_DELIVERY_CONSUMER_GROUP", "telegram-delivery")
    consumer_name: str = os.getenv("TELEGRAM_DELIVERY_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    batch_size: int = int(os.getenv("TELEGRAM_DELIVERY_BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("TELEGRAM_DELIVERY_BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("TELEGRAM_DELIVERY_PENDING_IDLE_MS", "30000"))
    dead_letter_stream: str = os.getenv("TELEGRAM_DELIVERY_DEAD_LETTER_STREAM", "stream:telegram-delivery-dead-letter")
    dead_letter_maxlen: int = int(os.getenv("TELEGRAM_DELIVERY_DEAD_LETTER_MAXLEN", "10000"))
    sent_key_prefix: str = os.getenv("TELEGRAM_DELIVERY_SENT_KEY_PREFIX", "delivery:telegram:sent")
    cooldown_key_prefix: str = os.getenv("TELEGRAM_DELIVERY_COOLDOWN_KEY_PREFIX", "delivery:telegram:cooldown")
    sent_ttl_seconds: int = int(os.getenv("TELEGRAM_DELIVERY_SENT_TTL_SECONDS", "604800"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()
