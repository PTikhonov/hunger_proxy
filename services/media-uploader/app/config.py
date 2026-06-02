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
    app_name: str = os.getenv("APP_NAME", "media-uploader")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    media_save_jobs_stream: str = os.getenv("MEDIA_SAVE_JOBS_STREAM", "stream:media-save-jobs")
    consumer_group: str = os.getenv("MEDIA_SAVE_CONSUMER_GROUP", "media-uploader")
    consumer_name: str = os.getenv("MEDIA_SAVE_CONSUMER_NAME", f"media-uploader-{socket.gethostname()}")
    batch_size: int = int(os.getenv("MEDIA_SAVE_BATCH_SIZE", "10"))
    block_ms: int = int(os.getenv("MEDIA_SAVE_BLOCK_MS", "5000"))
    pending_idle_ms: int = int(os.getenv("MEDIA_SAVE_PENDING_IDLE_MS", "30000"))
    ffupload_timeout_seconds: float = float(os.getenv("FFUPLOAD_TIMEOUT_SECONDS", "10"))
    delete_after_upload: bool = os.getenv("MEDIA_DELETE_AFTER_UPLOAD", "false").lower() in {"1", "true", "yes", "on"}
    log_level: str = _log_level()


settings = Settings()
