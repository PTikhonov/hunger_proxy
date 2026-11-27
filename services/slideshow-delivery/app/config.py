from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "slideshow-delivery")
    enabled: bool = _bool_env("SLIDESHOW_DELIVERY_ENABLED", True)
    host: str = os.getenv("SLIDESHOW_DELIVERY_HOST", "0.0.0.0")
    port: int = int(os.getenv("SLIDESHOW_DELIVERY_PORT", "8013"))
    service_token: str = os.getenv("SLIDESHOW_DELIVERY_TOKEN", "")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_config_url: str = os.getenv("REDIS_CONFIG_URL", "redis://redis-hot-state:6379/1")
    device_key_prefix: str = os.getenv("SLIDESHOW_DEVICE_KEY_PREFIX", "hunger:slideshow-devices:project")
    offers_stream: str = os.getenv("OFFERS_STREAM", "stream:offers")
    consumer_group: str = os.getenv("SLIDESHOW_DELIVERY_CONSUMER_GROUP", "slideshow-delivery")
    consumer_name: str = os.getenv("SLIDESHOW_DELIVERY_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    batch_size: int = int(os.getenv("SLIDESHOW_DELIVERY_BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("SLIDESHOW_DELIVERY_BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("SLIDESHOW_DELIVERY_PENDING_IDLE_MS", "30000"))
    dead_letter_stream: str = os.getenv("SLIDESHOW_DELIVERY_DEAD_LETTER_STREAM", "stream:slideshow-delivery-dead-letter")
    dead_letter_maxlen: int = int(os.getenv("SLIDESHOW_DELIVERY_DEAD_LETTER_MAXLEN", "10000"))
    sent_key_prefix: str = os.getenv("SLIDESHOW_DELIVERY_SENT_KEY_PREFIX", "delivery:slideshow:sent")
    cooldown_key_prefix: str = os.getenv("SLIDESHOW_DELIVERY_COOLDOWN_KEY_PREFIX", "delivery:slideshow:cooldown")
    sent_ttl_seconds: int = int(os.getenv("SLIDESHOW_DELIVERY_SENT_TTL_SECONDS", "604800"))
    http_timeout_seconds: float = float(os.getenv("SLIDESHOW_DELIVERY_TIMEOUT_SECONDS", "10"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()
