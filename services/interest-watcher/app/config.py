from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "interest-watcher")
    redis_stream_url: str = os.getenv("REDIS_STREAM_URL", "redis://redis-streams:6379/0")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_geometry_url: str = os.getenv("REDIS_GEOMETRY_URL", "redis://redis-hot-state:6379/1")
    observations_stream: str = os.getenv("IDENTITY_OBSERVATIONS_STREAM", "stream:identity-observations")
    consumer_group: str = os.getenv("IDENTITY_OBSERVATIONS_CONSUMER_GROUP", "interest-watcher")
    consumer_name: str = os.getenv("INTEREST_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    batch_size: int = int(os.getenv("INTEREST_BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("INTEREST_BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("INTEREST_PENDING_IDLE_MS", "30000"))
    dead_letter_stream: str = os.getenv("INTEREST_DEAD_LETTER_STREAM", "stream:interest-watcher-dead-letter")
    dead_letter_maxlen: int = int(os.getenv("INTEREST_DEAD_LETTER_MAXLEN", "10000"))
    interest_gap_seconds: float = float(os.getenv("INTEREST_GAP_SECONDS", "10"))
    interest_misses_before_reset: int = int(os.getenv("INTEREST_MISSES_BEFORE_RESET", "3"))
    person_resolve_timeout_seconds: float = float(os.getenv("PERSON_RESOLVE_TIMEOUT_SECONDS", "3"))
    person_resolve_interval_seconds: float = float(os.getenv("PERSON_RESOLVE_INTERVAL_SECONDS", "0.1"))
    person_ttl_seconds: int = int(os.getenv("PERSON_TTL_SECONDS", "3600"))
    extraction_api_url: str = os.getenv("EXTRACTION_API_URL", "http://127.0.0.1:18666/v2")
    extraction_timeout_seconds: float = float(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "5"))
    media_download_timeout_seconds: float = float(os.getenv("MEDIA_DOWNLOAD_TIMEOUT_SECONDS", "5"))
    extraction_image_field: str = os.getenv("EXTRACTION_IMAGE_FIELD", "sample")
    head_detector: str = os.getenv("HEAD_DETECTOR", "head")
    head_pose_attribute: str = os.getenv("HEAD_POSE_ATTRIBUTE", "head_headpose_unconstrained")
    head_height_meters: float = float(os.getenv("HEAD_HEIGHT_METERS", "0.25"))
    min_head_size: int = int(os.getenv("MIN_HEAD_SIZE", "30"))
    max_head_width_to_height_rate: float = float(os.getenv("MAX_HEAD_WIDTH_TO_HEIGHT_RATE", "1"))
    max_head_height_to_body_height: float = float(os.getenv("MAX_HEAD_HEIGHT_TO_BODY_HEIGHT", "0.5"))
    max_head_to_body_proportion: float = float(os.getenv("MAX_HEAD_TO_BODY_PROPORTION", "0.5"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()
