from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _csv_env(name: str, default: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in os.getenv(name, default).split(",") if item.strip())


def _log_level() -> str:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    if level not in {"INFO", "DEBUG"}:
        raise ValueError("LOG_LEVEL must be INFO or DEBUG")
    return level


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "visitor-state-processor")

    redis_stream_url: str = os.getenv("REDIS_STREAM_URL", "redis://localhost:6379/0")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://localhost:6380/0")

    detections_stream: str = os.getenv("DETECTIONS_STREAM", "stream:detections")
    detections_consumer_group: str = os.getenv("DETECTIONS_CONSUMER_GROUP", "visitor-state-processor")
    detections_consumer_name: str = os.getenv(
        "DETECTIONS_CONSUMER_NAME",
        f"visitor-state-processor-{socket.gethostname()}",
    )
    identity_observations_stream: str = os.getenv(
        "IDENTITY_OBSERVATIONS_STREAM",
        "stream:identity-observations",
    )
    stream_maxlen: int = int(os.getenv("STREAM_MAXLEN", "100000"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("PENDING_IDLE_MS", "30000"))

    extraction_api_url: str = os.getenv("EXTRACTION_API_URL", "http://192.168.1.25:18666/v2")
    extraction_timeout_seconds: float = float(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "5"))
    extraction_image_field: str = os.getenv("EXTRACTION_IMAGE_FIELD", "sample")

    face_extraction_detector: str = os.getenv("FACE_EXTRACTION_DETECTOR", os.getenv("EXTRACTION_DETECTOR", "face"))
    face_extraction_attributes: tuple[str, ...] = _csv_env(
        "FACE_EXTRACTION_ATTRIBUTES",
        os.getenv("EXTRACTION_ATTRIBUTES", "face_emben"),
    )
    face_embedding_field: str = os.getenv("FACE_EMBEDDING_FIELD", os.getenv("EXTRACTION_EMBEDDING_FIELD", "face_emben"))
    face_age_field: str = os.getenv("FACE_AGE_FIELD", "face_age")
    face_gender_field: str = os.getenv("FACE_GENDER_FIELD", "face_gender")
    face_extraction_need_age: bool = _bool_env("FACE_EXTRACTION_NEED_AGE", True)
    face_extraction_need_gender: bool = _bool_env("FACE_EXTRACTION_NEED_GENDER", True)

    silhouette_extraction_detector: str = os.getenv("SILHOUETTE_EXTRACTION_DETECTOR", "body")
    silhouette_extraction_attributes: tuple[str, ...] = _csv_env("SILHOUETTE_EXTRACTION_ATTRIBUTES", "body_emben")
    silhouette_embedding_field: str = os.getenv("SILHOUETTE_EMBEDDING_FIELD", "body_emben")
    silhouette_age_field: str = os.getenv("SILHOUETTE_AGE_FIELD", "body_age_gender")
    silhouette_gender_field: str = os.getenv("SILHOUETTE_GENDER_FIELD", "body_age_gender")
    silhouette_extraction_need_age: bool = _bool_env("SILHOUETTE_EXTRACTION_NEED_AGE", True)
    silhouette_extraction_need_gender: bool = _bool_env("SILHOUETTE_EXTRACTION_NEED_GENDER", True)
    extraction_roi: str | None = os.getenv("EXTRACTION_ROI")

    normalized_media_field: str = os.getenv("NORMALIZED_MEDIA_FIELD", "normalized")
    face_identity_confidence_threshold: float = float(
        os.getenv(
            "FACE_IDENTITY_CONFIDENCE_THRESHOLD",
            os.getenv("IDENTITY_CONFIDENCE_THRESHOLD", "0.71"),
        )
    )
    silhouette_identity_confidence_threshold: float = float(
        os.getenv(
            "SILHOUETTE_IDENTITY_CONFIDENCE_THRESHOLD",
            os.getenv("IDENTITY_CONFIDENCE_THRESHOLD", "0.73"),
        )
    )
    identity_ttl_seconds: int = int(os.getenv("IDENTITY_TTL_SECONDS", "3600"))
    presence_gap_seconds: float = float(os.getenv("PRESENCE_GAP_SECONDS", "10"))

    log_level: str = _log_level()


settings = Settings()
