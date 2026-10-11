from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import Settings


class HeadNotDetectedError(ValueError):
    pass


class ExtractionClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(timeout=settings.extraction_timeout_seconds)

    async def extract_head(
        self,
        image_bytes: bytes,
        filename: str,
        content_type: str,
        body_width: float,
        body_height: float,
    ) -> tuple[list[float], dict[str, float]]:
        request = {
            "requests": [{
                "image": f"multipart:{self._settings.extraction_image_field}",
                "detector": self._settings.head_detector,
                "attributes": [self._settings.head_pose_attribute],
            }]
        }
        response = await self._client.post(
            self._settings.extraction_api_url,
            files={
                self._settings.extraction_image_field: (filename, image_bytes, content_type),
                "request": (None, json.dumps(request), "application/json"),
            },
        )
        response.raise_for_status()
        return self._parse(response.json(), body_width=body_width, body_height=body_height)

    def _parse(
        self,
        payload: dict[str, Any],
        body_width: float,
        body_height: float,
    ) -> tuple[list[float], dict[str, float]]:
        responses = payload.get("responses") or []
        if not responses:
            raise ValueError("Extraction API returned no responses")
        heads = ((responses[0].get("objects") or {}).get("head") or [])
        if not heads:
            raise HeadNotDetectedError("Extraction API did not detect a head")
        if body_width <= 0 or body_height <= 0:
            raise ValueError("body crop dimensions must be positive")
        eligible_heads = [
            head
            for head in heads
            if self._is_eligible_head(head, body_width=body_width, body_height=body_height)
        ]
        if not eligible_heads:
            raise HeadNotDetectedError(
                "All head detections violate configured geometry constraints: "
                f"min_head_size={self._settings.min_head_size}, "
                "max_head_width_to_height_rate="
                f"{self._settings.max_head_width_to_height_rate}, "
                "max_head_height_to_body_height="
                f"{self._settings.max_head_height_to_body_height}, "
                "max_head_to_body_proportion="
                f"{self._settings.max_head_to_body_proportion}"
            )
        head = max(eligible_heads, key=lambda item: float(item.get("detection_score") or 0.0))
        bbox = head.get("bbox") or {}
        attribute = ((head.get("attributes") or {}).get(self._settings.head_pose_attribute) or {})
        result = attribute.get("result") or {}
        return (
            [float(bbox[name]) for name in ("left", "top", "right", "bottom")],
            {name: float(result[name]) for name in ("pitch", "roll", "yaw")},
        )

    def _is_eligible_head(self, head: dict[str, Any], body_width: float, body_height: float) -> bool:
        width, height = self._head_dimensions(head)
        if width < self._settings.min_head_size or height < self._settings.min_head_size:
            return False
        if height <= 0 or width / height > self._settings.max_head_width_to_height_rate:
            return False
        if height / body_height > self._settings.max_head_height_to_body_height:
            return False
        return width * height / (body_width * body_height) <= self._settings.max_head_to_body_proportion

    @staticmethod
    def _head_dimensions(head: dict[str, Any]) -> tuple[float, float]:
        bbox = head.get("bbox") or {}
        width = max(0.0, float(bbox.get("right", 0)) - float(bbox.get("left", 0)))
        height = max(0.0, float(bbox.get("bottom", 0)) - float(bbox.get("top", 0)))
        return width, height

    async def aclose(self) -> None:
        await self._client.aclose()
