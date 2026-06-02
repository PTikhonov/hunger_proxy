from __future__ import annotations

import base64
import json
import logging
import struct
from typing import Any

import httpx

from app.config import Settings
from app.schemas import ExtractionResult


logger = logging.getLogger(__name__)


class ExtractionClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(timeout=settings.extraction_timeout_seconds)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def extract_embedding(
        self,
        detection_type: str,
        image_bytes: bytes,
        filename: str = "normalized.jpg",
        content_type: str = "image/jpeg",
    ) -> ExtractionResult:
        detector, attributes, embedding_field = self._profile(detection_type)
        request_fields: dict[str, Any] = {
            "detector": detector,
            "image": f"multipart:{self._settings.extraction_image_field}",
        }
        if attributes:
            request_fields["attributes"] = list(attributes)
        else:
            request_fields["need_facen"] = True

        if self._settings.extraction_roi:
            request_fields["roi"] = self._settings.extraction_roi

        request_payload = {"requests": [request_fields]}
        response = await self._client.post(
            self._settings.extraction_api_url,
            data={"request": json.dumps(request_payload)},
            files={
                self._settings.extraction_image_field: (
                    filename,
                    image_bytes,
                    content_type,
                )
            },
        )
        response.raise_for_status()

        data = response.json()
        entity = _first_entity(data, detection_type)
        embedding_value = _embedding_value(entity, embedding_field)
        if embedding_value is None:
            logger.info(
                "Extraction response does not contain embedding field=%s status=%s body=%s",
                embedding_field,
                response.status_code,
                response.text[:1000],
            )
            raise ValueError(f"Extraction response does not contain {embedding_field}")

        embedding = decode_embedding(embedding_value)
        if not embedding:
            raise ValueError("Extraction response contains empty embedding")

        confidence = _number_or_none(_find_key(entity, "detection_score") or _find_key(entity, "confidence"))
        gender = _extract_gender(entity)
        age = _extract_age(entity)
        body_age = _extract_body_age(entity)
        body_gender = _extract_body_gender(entity)

        logger.debug(
            "Extracted embedding dimensions=%s confidence=%s age=%s gender=%s body_age=%s body_gender=%s",
            len(embedding),
            confidence,
            age,
            gender,
            body_age,
            body_gender,
        )
        return ExtractionResult(
            embedding=embedding,
            confidence=confidence,
            age=age,
            gender=gender,
            body_age=body_age,
            body_gender=body_gender,
            raw=entity if isinstance(entity, dict) else {"entity": entity},
        )

    def _profile(self, detection_type: str) -> tuple[str, tuple[str, ...], str]:
        if detection_type == "face":
            return (
                self._settings.face_extraction_detector,
                self._attributes(
                    base=self._settings.face_extraction_attributes,
                    need_age=self._settings.face_extraction_need_age,
                    age_field=self._settings.face_age_field,
                    need_gender=self._settings.face_extraction_need_gender,
                    gender_field=self._settings.face_gender_field,
                ),
                self._settings.face_embedding_field,
            )
        if detection_type == "silhouette":
            return (
                self._settings.silhouette_extraction_detector,
                self._attributes(
                    base=self._settings.silhouette_extraction_attributes,
                    need_age=self._settings.silhouette_extraction_need_age,
                    age_field=self._settings.silhouette_age_field,
                    need_gender=self._settings.silhouette_extraction_need_gender,
                    gender_field=self._settings.silhouette_gender_field,
                ),
                self._settings.silhouette_embedding_field,
            )
        raise ValueError(f"Unsupported detection type for extraction: {detection_type}")

    def _attributes(
        self,
        base: tuple[str, ...],
        need_age: bool,
        age_field: str,
        need_gender: bool,
        gender_field: str,
    ) -> tuple[str, ...]:
        attributes = list(base)
        if need_age and age_field not in attributes:
            attributes.append(age_field)
        if need_gender and gender_field not in attributes:
            attributes.append(gender_field)
        return tuple(attributes)


def _first_entity(data: Any, detection_type: str) -> Any:
    if not isinstance(data, dict):
        return data

    responses = data.get("responses")
    if isinstance(responses, list) and responses:
        response = responses[0]
        if isinstance(response, dict):
            objects = response.get("objects")
            if isinstance(objects, dict):
                preferred_keys = ("face",) if detection_type == "face" else ("silhouette", "body", "person")
                for key in preferred_keys:
                    items = objects.get(key)
                    if isinstance(items, list) and items:
                        return items[0]
                for items in objects.values():
                    if isinstance(items, list) and items:
                        return items[0]
            for key in ("faces", "objects", "detections", "results"):
                items = response.get(key)
                if isinstance(items, list) and items:
                    return items[0]
            return response

    return data


def _embedding_value(entity: Any, embedding_field: str) -> Any:
    value = _find_key(entity, embedding_field)
    if isinstance(value, dict) and "result" in value:
        return value["result"]
    if value is not None:
        return value
    return _find_key(entity, "result")


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


def _extract_gender(entity: Any) -> str | None:
    gender = _attribute_result(entity, "face_gender") or _extract_body_gender(entity)
    if isinstance(gender, dict):
        nested_gender = _find_key(gender, "gender")
        if nested_gender is not None:
            gender = nested_gender

    if isinstance(gender, list) and gender:
        best = max(gender, key=lambda item: item.get("confidence", 0) if isinstance(item, dict) else 0)
        if isinstance(best, dict) and best.get("name"):
            return str(best["name"])

    gender = _attribute_result(entity, "gender") or _find_key(entity, "gender")
    if isinstance(gender, dict):
        for key in ("gender", "value", "name"):
            nested = gender.get(key)
            if nested:
                return str(nested)
    if gender:
        return str(gender)
    return None


def _extract_age(entity: Any) -> float | int | None:
    age = _attribute_result(entity, "face_age")
    if age is None:
        body_age_gender = _attribute_result(entity, "body_age_gender")
        if isinstance(body_age_gender, dict):
            age = _find_key(body_age_gender, "age")
    return _number_or_none(age or _find_key(entity, "age"))


def _extract_body_age(entity: Any) -> str | None:
    body_age_gender = _attribute_result(entity, "body_age_gender")
    if not isinstance(body_age_gender, dict):
        return None
    return _best_confidence_name(body_age_gender.get("age_group") or _find_key(body_age_gender, "age_group"))


def _extract_body_gender(entity: Any) -> str | None:
    body_age_gender = _attribute_result(entity, "body_age_gender")
    if not isinstance(body_age_gender, dict):
        return None
    return _best_confidence_name(body_age_gender.get("gender") or _find_key(body_age_gender, "gender"))


def _best_confidence_name(value: Any) -> str | None:
    if not isinstance(value, list) or not value:
        return None
    best = max(value, key=lambda item: item.get("confidence", 0) if isinstance(item, dict) else 0)
    if isinstance(best, dict) and best.get("name"):
        return str(best["name"])
    return None


def _attribute_result(entity: Any, attribute_name: str) -> Any:
    attribute = _find_key(entity, attribute_name)
    if isinstance(attribute, dict) and "result" in attribute:
        return attribute["result"]
    return attribute
