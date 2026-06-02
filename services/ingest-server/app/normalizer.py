from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from starlette.datastructures import UploadFile

from app.schemas import DetectionEvent


@dataclass
class IngestedRequest:
    event: DetectionEvent
    blobs: dict[str, bytes]


JSON_KEYS = {
    "camera_id",
    "cam_id",
    "label_cam_id",
    "timestamp",
    "track_id",
    "face_id",
    "silhouette_id",
    "bbox",
    "age",
    "gender",
    "head_pose",
    "detectorParams",
    "detector_params",
    "labels",
}


def _json_or_raw(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = value.strip()
    if not value:
        return value
    if value[0] not in "[{":
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _as_number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, int | float):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _normalize_mapping(data: dict[str, Any], media: dict[str, Any] | None = None) -> DetectionEvent:
    parsed = {key: _json_or_raw(value) for key, value in data.items()}
    detector_params = parsed.get("detector_params", parsed.get("detectorParams"))
    labels = parsed.get("labels") if isinstance(parsed.get("labels"), dict) else None

    camera_id = (
        parsed.get("camera_id")
        or parsed.get("cam_id")
        or parsed.get("label_cam_id")
        or (labels or {}).get("cam_id")
    )

    return DetectionEvent(
        camera_id=str(camera_id) if camera_id is not None else None,
        event_timestamp=parsed.get("timestamp"),
        track_id=parsed.get("track_id"),
        face_id=parsed.get("face_id"),
        silhouette_id=parsed.get("silhouette_id"),
        bbox=parsed.get("bbox"),
        age=_as_number(parsed.get("age")),
        gender=parsed.get("gender"),
        head_pose=parsed.get("head_pose") if isinstance(parsed.get("head_pose"), dict) else None,
        detector_params=detector_params,
        labels=labels,
        media=media or {},
        raw={key: value for key, value in parsed.items() if key in JSON_KEYS or key.startswith("label_")},
    )


async def normalize_request(request: Request) -> IngestedRequest:
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("JSON request body must be an object")
        return IngestedRequest(event=_normalize_mapping(body), blobs={})

    if "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
        form = await request.form()
        fields: dict[str, Any] = {}
        media: dict[str, Any] = {}
        blobs: dict[str, bytes] = {}

        for key, value in form.multi_items():
            if isinstance(value, UploadFile):
                content = await value.read()
                blobs[key] = content
                media[key] = {
                    "filename": value.filename,
                    "content_type": value.content_type,
                    "size": len(content),
                }
                await value.close()
            else:
                fields[key] = value

        return IngestedRequest(event=_normalize_mapping(fields, media), blobs=blobs)

    raise ValueError(f"Unsupported content type: {content_type or '<empty>'}")
