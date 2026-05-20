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
    log_level: str = _log_level()


settings = Settings()
