from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl


class Credentials(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=255)


class DisplayRef(BaseModel):
    id: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=100)
    location_id: int = Field(gt=0)


class DeviceSnapshotItem(BaseModel):
    id: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=120)
    api_url: HttpUrl
    credentials: Credentials
    enabled: bool
    display_ids: list[int] = []
    displays: list[DisplayRef] = []
    group_ids: list[int] = []


class DeviceSnapshot(BaseModel):
    schema_version: int = Field(eq=1)
    project_id: int = Field(gt=0)
    revision: int = Field(gt=0)
    updated_at: datetime
    devices: list[DeviceSnapshotItem]


class ApplyResult(BaseModel):
    status: str
    project_id: int
    revision: int
