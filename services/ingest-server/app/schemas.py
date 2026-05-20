from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class DetectionEvent(BaseModel):
    event_type: str = "person_detected"
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    source: str = "video-server"
    camera_id: str | None = None
    event_timestamp: str | None = None

    track_id: str | None = None
    face_id: str | None = None
    silhouette_id: str | None = None

    bbox: Any | None = None
    age: int | float | None = None
    gender: str | None = None
    head_pose: dict[str, Any] | None = None
    detector_params: dict[str, Any] | str | None = None
    labels: dict[str, Any] | None = None

    media: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    def stream_fields(self) -> dict[str, str]:
        payload = self.model_dump_json()
        return {
            "event_type": self.event_type,
            "event_id": self.event_id,
            "camera_id": self.camera_id or "",
            "event_timestamp": self.event_timestamp or "",
            "received_at": self.received_at.isoformat(),
            "payload": payload,
        }

