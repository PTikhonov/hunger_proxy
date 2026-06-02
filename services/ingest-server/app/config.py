import os
from dataclasses import dataclass


def _log_level() -> str:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    if level not in {"INFO", "DEBUG"}:
        raise ValueError("LOG_LEVEL must be INFO or DEBUG")
    return level


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "ingest-server")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    detections_stream: str = os.getenv("DETECTIONS_STREAM", "stream:detections")
    stream_maxlen: int = int(os.getenv("STREAM_MAXLEN", "100000"))
    normalized_media_field: str = os.getenv("NORMALIZED_MEDIA_FIELD", "normalized")
    full_frame_media_field: str = os.getenv("FULL_FRAME_MEDIA_FIELD", "photo")
    media_key_prefix: str = os.getenv("MEDIA_KEY_PREFIX", "media:detection")
    media_ttl_seconds: int = int(os.getenv("MEDIA_TTL_SECONDS", "120"))
    media_save_jobs_stream: str = os.getenv("MEDIA_SAVE_JOBS_STREAM", "stream:media-save-jobs")
    ffupload_base_url: str = os.getenv("FFUPLOAD_BASE_URL", "http://192.168.1.25:3333/uploads")
    media_url_base: str = os.getenv("MEDIA_URL_BASE", os.getenv("FFUPLOAD_BASE_URL", "http://192.168.1.25:3333/uploads"))
    log_level: str = _log_level()


settings = Settings()
