from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "xibo-audio-delivery")
    enabled: bool = _bool_env("XIBO_DELIVERY_ENABLED", _bool_env("XIBO_AUDIO_DELIVERY_ENABLED", False))
    delivery_type: str = os.getenv("XIBO_DELIVERY_TYPE", "xibo_audio")
    delivery_label: str = os.getenv("XIBO_DELIVERY_LABEL", "audio")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_rules_url: str = os.getenv("REDIS_RULES_URL", "redis://redis-hot-state:6379/1")
    promotion_rules_key_prefix: str = os.getenv("PROMOTION_RULES_KEY_PREFIX", "hunger:promotion-rules:location")
    promotion_rules_revision_key_prefix: str = os.getenv(
        "PROMOTION_RULES_REVISION_KEY_PREFIX", "hunger:promotion-rules:location"
    )
    offers_stream: str = os.getenv("OFFERS_STREAM", "stream:offers")
    consumer_group: str = os.getenv("XIBO_DELIVERY_CONSUMER_GROUP", os.getenv("XIBO_AUDIO_DELIVERY_CONSUMER_GROUP", "xibo-audio-delivery"))
    consumer_name: str = os.getenv("XIBO_DELIVERY_CONSUMER_NAME", os.getenv("XIBO_AUDIO_DELIVERY_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}"))
    batch_size: int = int(os.getenv("XIBO_DELIVERY_BATCH_SIZE", os.getenv("XIBO_AUDIO_DELIVERY_BATCH_SIZE", "10")))
    block_ms: int = int(os.getenv("XIBO_DELIVERY_BLOCK_MS", os.getenv("XIBO_AUDIO_DELIVERY_BLOCK_MS", "5000")))
    pending_idle_ms: int = int(os.getenv("XIBO_DELIVERY_PENDING_IDLE_MS", os.getenv("XIBO_AUDIO_DELIVERY_PENDING_IDLE_MS", "30000")))
    dead_letter_stream: str = os.getenv(
        "XIBO_DELIVERY_DEAD_LETTER_STREAM", os.getenv("XIBO_AUDIO_DELIVERY_DEAD_LETTER_STREAM", "stream:xibo-audio-delivery-dead-letter")
    )
    dead_letter_maxlen: int = int(os.getenv("XIBO_DELIVERY_DEAD_LETTER_MAXLEN", os.getenv("XIBO_AUDIO_DELIVERY_DEAD_LETTER_MAXLEN", "10000")))
    sent_key_prefix: str = os.getenv("XIBO_DELIVERY_SENT_KEY_PREFIX", os.getenv("XIBO_AUDIO_DELIVERY_SENT_KEY_PREFIX", "delivery:xibo:audio:sent"))
    cooldown_key_prefix: str = os.getenv("XIBO_DELIVERY_COOLDOWN_KEY_PREFIX", os.getenv("XIBO_AUDIO_DELIVERY_COOLDOWN_KEY_PREFIX", "delivery:xibo:audio:cooldown"))
    sent_ttl_seconds: int = int(os.getenv("XIBO_DELIVERY_SENT_TTL_SECONDS", os.getenv("XIBO_AUDIO_DELIVERY_SENT_TTL_SECONDS", "604800")))
    timeout_seconds: float = float(os.getenv("XIBO_TIMEOUT_SECONDS", "10"))
    # Fallback for deployments whose hunger-head snapshot does not yet contain delivery.xibo.
    xibo_cms_url: str = os.getenv("XIBO_CMS_URL", "")
    xibo_client_id: str = os.getenv("XIBO_CLIENT_ID", "")
    xibo_client_secret: str = os.getenv("XIBO_CLIENT_SECRET", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()
