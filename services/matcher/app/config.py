from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _log_level() -> str:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    if level not in {"INFO", "DEBUG"}:
        raise ValueError("LOG_LEVEL must be INFO or DEBUG")
    return level


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "matcher")

    redis_stream_url: str = os.getenv("REDIS_STREAM_URL", "redis://localhost:6379/0")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://localhost:6380/0")

    identity_observations_stream: str = os.getenv("IDENTITY_OBSERVATIONS_STREAM", "stream:identity-observations")
    identity_observations_consumer_group: str = os.getenv("IDENTITY_OBSERVATIONS_CONSUMER_GROUP", "matcher")
    identity_observations_consumer_name: str = os.getenv(
        "IDENTITY_OBSERVATIONS_CONSUMER_NAME",
        f"matcher-{socket.gethostname()}",
    )
    person_events_stream: str = os.getenv("PERSON_EVENTS_STREAM", "stream:person-events")
    stream_maxlen: int = int(os.getenv("STREAM_MAXLEN", "100000"))
    batch_size: int = int(os.getenv("MATCHER_BATCH_SIZE", "20"))
    block_ms: int = int(os.getenv("MATCHER_BLOCK_MS", "1000"))
    pending_idle_ms: int = int(os.getenv("MATCHER_PENDING_IDLE_MS", "30000"))

    matcher_lookback_seconds: float = float(os.getenv("MATCHER_LOOKBACK_SECONDS", "10"))
    matcher_lookahead_seconds: float = float(os.getenv("MATCHER_LOOKAHEAD_SECONDS", "2"))
    matcher_observation_ttl_seconds: int = int(os.getenv("MATCHER_OBSERVATION_TTL_SECONDS", "600"))
    matcher_face_body_confidence_threshold: float = float(os.getenv("MATCHER_FACE_BODY_CONFIDENCE_THRESHOLD", "0.78"))
    matcher_face_detection_quality_threshold: float = float(
        os.getenv("MATCHER_FACE_DETECTION_QUALITY_THRESHOLD", "0.60")
    )
    person_ttl_seconds: int = int(os.getenv("PERSON_TTL_SECONDS", "3600"))

    extraction_api_url: str = os.getenv("EXTRACTION_API_URL", "http://192.168.1.25:18666/v2")
    extraction_timeout_seconds: float = float(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "5"))
    extraction_image_field: str = os.getenv("EXTRACTION_IMAGE_FIELD", "sample")
    extraction_face_detector: str = os.getenv("MATCHER_FACE_EXTRACTION_DETECTOR", "face")
    extraction_face_embedding_field: str = os.getenv("MATCHER_FACE_EMBEDDING_FIELD", "face_emben")

    log_level: str = _log_level()


settings = Settings()
