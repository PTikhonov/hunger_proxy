from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "state-visualizer")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://localhost:6380/0")
    identity_scan_count: int = int(os.getenv("IDENTITY_SCAN_COUNT", "500"))
    poll_interval_ms: int = int(os.getenv("STATE_VISUALIZER_POLL_INTERVAL_MS", "1500"))
    media_delete_timeout_seconds: float = float(os.getenv("MEDIA_DELETE_TIMEOUT_SECONDS", "10"))
    media_delete_concurrency: int = int(os.getenv("MEDIA_DELETE_CONCURRENCY", "10"))


settings = Settings()
