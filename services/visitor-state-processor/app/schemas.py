from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class DetectionPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str
    event_type: str | None = None
    received_at: datetime | str | None = None
    source: str | None = None
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


class ExtractionResult(BaseModel):
    embedding: list[float]
    confidence: float | None = None
    age: int | float | None = None
    gender: str | None = None
    body_age: str | None = None
    body_gender: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class IdentityObservation(BaseModel):
    event_type: str = "identity_observed"
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source_event_id: str
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    identity_id: str
    detection_type: str
    is_new_identity: bool
    match_confidence: float | None = None
    extraction_confidence: float | None = None

    camera_id: str | None = None
    event_timestamp: str | None = None
    track_id: str | None = None
    bbox: Any | None = None
    age: int | float | None = None
    gender: str | None = None
    media: dict[str, Any] = Field(default_factory=dict)
    raw_detection: dict[str, Any] = Field(default_factory=dict)

    def stream_fields(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "event_id": self.event_id,
            "source_event_id": self.source_event_id,
            "identity_id": self.identity_id,
            "detection_type": self.detection_type,
            "camera_id": self.camera_id or "",
            "event_timestamp": self.event_timestamp or "",
            "observed_at": self.observed_at.isoformat(),
            "payload": self.model_dump_json(),
        }
