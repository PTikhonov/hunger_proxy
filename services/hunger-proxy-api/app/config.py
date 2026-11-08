from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "hunger-proxy-api")
    redis_geometry_url: str = os.getenv("REDIS_GEOMETRY_URL", "redis://localhost:6380/1")
    geometry_key_prefix: str = os.getenv("GEOMETRY_KEY_PREFIX", "hunger:geometry:camera")
    geometry_camera_index: str = os.getenv("GEOMETRY_CAMERA_INDEX", "hunger:geometry:cameras")
    geometry_changed_channel: str = os.getenv("GEOMETRY_CHANGED_CHANNEL", "hunger:geometry:changed")
    promotion_rules_key_prefix: str = os.getenv("PROMOTION_RULES_KEY_PREFIX", "hunger:promotion-rules:location")
    promotion_rules_revision_key_prefix: str = os.getenv(
        "PROMOTION_RULES_REVISION_KEY_PREFIX", "hunger:promotion-rules:location"
    )
    promotion_rules_revision_ttl_seconds: int = int(os.getenv("PROMOTION_RULES_REVISION_TTL_SECONDS", "604800"))
    promotion_rules_location_index: str = os.getenv("PROMOTION_RULES_LOCATION_INDEX", "hunger:promotion-rules:locations")
    promotion_rules_changed_channel: str = os.getenv("PROMOTION_RULES_CHANGED_CHANNEL", "hunger:promotion-rules:changed")
    service_token: str = os.getenv("HUNGER_PROXY_API_TOKEN", "")


settings = Settings()
