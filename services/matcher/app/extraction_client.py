from __future__ import annotations

import base64
import json
import logging
import struct
from dataclasses import dataclass
from typing import Any

import httpx

from app.config import Settings


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FaceExtractionResult:
    embedding: list[float]
    confidence: float | None
    raw: dict[str, Any]


class FaceNotFoundError(ValueError):
    pass


class ExtractionClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(timeout=settings.extraction_timeout_seconds)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def extract_face_embedding(
        self,
        image_bytes: bytes,
        filename: str = "normalized.jpg",
        content_type: str = "image/jpeg",
    ) -> FaceExtractionResult:
        request_payload = {
            "requests": [
                {
                    "image": f"multipart:{self._settings.extraction_image_field}",
                    "detector": self._settings.extraction_face_detector,
                    "attributes": [self._settings.extraction_face_embedding_field],
                }
            ]
        }
        response = await self._client.post(
            self._settings.extraction_api_url,
            data={"request": json.dumps(request_payload)},
            files={self._settings.extraction_image_field: (filename, image_bytes, content_type)},
        )
        response.raise_for_status()

        data = response.json()
        entity = _first_face_entity(data)
        if entity is None:
            raise FaceNotFoundError("Face was not found in image")

        embedding_value = _embedding_value(entity, self._settings.extraction_face_embedding_field)
        if embedding_value is None:
            logger.debug("Face extraction response has no embedding body=%s", response.text[:1000])
            raise ValueError("Face extraction response does not contain embedding")

        embedding = decode_embedding(embedding_value)
        confidence = _number_or_none(_find_key(entity, "detection_score") or _find_key(entity, "confidence"))
        return FaceExtractionResult(
            embedding=embedding,
            confidence=confidence,
            raw=entity if isinstance(entity, dict) else {"entity": entity},
        )


def _first_face_entity(data: Any) -> Any | None:
    if not isinstance(data, dict):
        return None
    responses = data.get("responses")
    if isinstance(responses, list) and responses:
        response = responses[0]
        if isinstance(response, dict):
            objects = response.get("objects")
            if isinstance(objects, dict):
                faces = objects.get("face")
                if isinstance(faces, list) and faces:
                    return faces[0]
    return None


def _embedding_value(entity: Any, embedding_field: str) -> Any:
    value = _find_key(entity, embedding_field)
    if isinstance(value, dict) and "result" in value:
        return value["result"]
    return value


def _find_key(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for nested in value.values():
            found = _find_key(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_key(item, key)
            if found is not None:
                return found
    return None


def decode_embedding(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]

    if isinstance(value, str):
        stripped = value.strip()
        try:
            decoded = base64.b64decode(stripped, validate=True)
            if len(decoded) % 4 == 0:
                return [item[0] for item in struct.iter_unpack("<f", decoded)]
        except Exception:
            pass

        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [float(item) for item in parsed]

    raise ValueError("Unsupported embedding format")


def _number_or_none(value: Any) -> float | int | None:
    if value is None or value == "":
        return None
    if isinstance(value, int | float):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number
