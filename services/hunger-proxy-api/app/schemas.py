from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CameraParameters(BaseModel):
    model_config = ConfigDict(extra="allow")

    focal_length_in_mm: float = Field(gt=0)
    sensor_width_in_mm: float = Field(gt=0)
    sensor_height_in_mm: float = Field(gt=0)
    frame_width: int = Field(gt=0)
    frame_height: int = Field(gt=0)


class ShelfGeometry(BaseModel):
    model_config = ConfigDict(extra="allow")

    shelf_id: str | int
    name: str | None = None
    category_id: str | int | None = None
    category_2_id: str | int | None = None
    side: str | None = None
    side_2: str | None = None
    viewing_direction: Literal["normal", "opposite", "both"] = "both"
    points: list[tuple[float, float, float]]

    @model_validator(mode="before")
    @classmethod
    def infer_legacy_viewing_direction(cls, value: Any) -> Any:
        if isinstance(value, dict) and "viewing_direction" not in value:
            value = dict(value)
            sides = {str(value.get(name) or "").lower() for name in ("side", "side_2")}
            sides.discard("")
            if "ab" in sides or {"a", "b"}.issubset(sides):
                value["viewing_direction"] = "both"
            else:
                value["viewing_direction"] = {
                    "a": "normal",
                    "b": "opposite",
                }.get(next(iter(sides), ""), "both")
        return value

    @field_validator("points")
    @classmethod
    def validate_points(cls, points: list[tuple[float, float, float]]) -> list[tuple[float, float, float]]:
        if len(points) < 3:
            raise ValueError("a shelf polygon must contain at least three points")
        if len(set(points)) < 3:
            raise ValueError("a shelf polygon must contain at least three distinct points")
        return points


class CameraGeometrySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1)
    camera_id: str | int
    revision: int = Field(ge=1)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    camera: CameraParameters
    shelves: list[ShelfGeometry] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_shelves(self) -> "CameraGeometrySnapshot":
        shelf_ids = [str(shelf.shelf_id) for shelf in self.shelves]
        if len(shelf_ids) != len(set(shelf_ids)):
            raise ValueError("shelf_id values must be unique within a camera snapshot")
        return self


class CameraGeometryTombstone(BaseModel):
    schema_version: int = Field(default=1, ge=1)
    camera_id: str | int
    revision: int = Field(ge=1)
    deleted: Literal[True] = True
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApplyResult(BaseModel):
    status: Literal["created", "updated", "unchanged", "deleted"]
    camera_id: str
    revision: int
    checksum: str


class GeometryRecord(BaseModel):
    camera_id: str
    revision: int
    schema_version: int
    checksum: str
    updated_at: str
    deleted: bool
    payload: dict[str, Any]


class GeometryListItem(BaseModel):
    camera_id: str
    revision: int
    updated_at: str
    deleted: bool
