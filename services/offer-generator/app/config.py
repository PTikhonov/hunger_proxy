from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "offer-generator")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_rules_url: str = os.getenv("REDIS_RULES_URL", "redis://redis-hot-state:6379/1")
    input_stream: str = os.getenv("INTEREST_THRESHOLD_EVENTS_STREAM", "stream:interest-threshold-events")
    consumer_group: str = os.getenv("OFFER_GENERATOR_CONSUMER_GROUP", "offer-generator")
    consumer_name: str = os.getenv("OFFER_GENERATOR_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    output_stream: str = os.getenv("OFFERS_STREAM", "stream:offers")
    output_maxlen: int = int(os.getenv("OFFERS_STREAM_MAXLEN", "100000"))
    dead_letter_stream: str = os.getenv("OFFER_GENERATOR_DEAD_LETTER_STREAM", "stream:offer-generator-dead-letter")
    dead_letter_maxlen: int = int(os.getenv("OFFER_GENERATOR_DEAD_LETTER_MAXLEN", "10000"))
    batch_size: int = int(os.getenv("OFFER_GENERATOR_BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("OFFER_GENERATOR_BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("OFFER_GENERATOR_PENDING_IDLE_MS", "30000"))
    promotion_rules_key_prefix: str = os.getenv("PROMOTION_RULES_KEY_PREFIX", "hunger:promotion-rules:location")
    promotion_rules_revision_key_prefix: str = os.getenv(
        "PROMOTION_RULES_REVISION_KEY_PREFIX", "hunger:promotion-rules:location"
    )
    offer_dedupe_prefix: str = os.getenv("OFFER_DEDUPE_PREFIX", "offer:dedupe")
    offer_state_prefix: str = os.getenv("OFFER_STATE_PREFIX", "offer")
    offer_min_dedupe_seconds: int = int(os.getenv("OFFER_MIN_DEDUPE_SECONDS", "60"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()
