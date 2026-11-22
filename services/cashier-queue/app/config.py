from __future__ import annotations
import os, socket
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    redis_stream_url: str = os.getenv("REDIS_STREAM_URL", "redis://redis-streams:6379/0")
    redis_hot_state_url: str = os.getenv("REDIS_HOT_STATE_URL", "redis://redis-hot-state:6379/0")
    redis_registry_url: str = os.getenv("REDIS_REGISTRY_URL", "redis://redis-hot-state:6379/1")
    observations_stream: str = os.getenv("IDENTITY_OBSERVATIONS_STREAM", "stream:identity-observations")
    offers_stream: str = os.getenv("OFFERS_STREAM", "stream:offers")
    group: str = os.getenv("CASHIER_QUEUE_CONSUMER_GROUP", "cashier-queue")
    consumer: str = os.getenv("CASHIER_QUEUE_CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    camera_registry_key: str = os.getenv("CASH_REGISTER_CAMERAS_KEY", "hunger:cash-register-cameras")
    queue_ttl: int = int(os.getenv("CASHIER_QUEUE_TTL_SECONDS", "30"))
    pending_idle_ms: int = int(os.getenv("CASHIER_QUEUE_PENDING_IDLE_MS", "10000"))
    show_images: bool = os.getenv("CASHIER_QUEUE_SHOW_IMAGES", "true").lower() in {"1", "true", "yes", "on"}
    session_secret: str = os.getenv("CASHIER_QUEUE_SESSION_SECRET", "change-me")
    hunger_head_url: str = os.getenv("HUNGER_HEAD_URL", "")
    hunger_head_token: str = os.getenv("HUNGER_HEAD_SERVICE_TOKEN", "")
settings = Settings()
