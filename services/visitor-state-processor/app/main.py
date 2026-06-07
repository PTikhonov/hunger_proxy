from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from time import time
from typing import Any

from redis.asyncio import Redis

from app.config import settings
from app.extraction_client import ExtractionClient
from app.identity import IdentityRegistry
from app.schemas import DetectionPayload, IdentityObservation
from app.streams import StreamClient


logger = logging.getLogger(settings.app_name)


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)


class VisitorStateProcessor:
    def __init__(
        self,
        stream_redis: Redis,
        media_redis: Redis,
        hot_state_redis: Redis,
        extraction_client: ExtractionClient,
    ) -> None:
        self._stream = StreamClient(stream_redis, settings)
        self._media_redis = media_redis
        self._hot_state_redis = hot_state_redis
        self._extraction_client = extraction_client
        self._identities = IdentityRegistry(
            thresholds={
                "face": settings.face_identity_confidence_threshold,
                "silhouette": settings.silhouette_identity_confidence_threshold,
            },
            ttl_seconds=settings.identity_ttl_seconds,
        )

    async def run(self) -> None:
        await self._stream.ensure_group()
        logger.info(
            "Started %s input=%s group=%s output=%s hot_state=%s extraction_api=%s",
            settings.app_name,
            settings.detections_stream,
            settings.detections_consumer_group,
            settings.identity_observations_stream,
            settings.redis_hot_state_url,
            settings.extraction_api_url,
        )
        while True:
            messages = await self._stream.read()
            for message_id, fields in messages:
                await self._handle_message(message_id, fields)

    async def _handle_message(self, message_id: str, fields: dict[str, str]) -> None:
        try:
            payload = self._parse_detection_payload(fields)
            detection_type = self._detect_type(payload)
            media_info = self._normalized_media_info(payload.media)
            redis_key = media_info.get("redis_key")
            if not redis_key:
                logger.info("Detection has no normalized redis_key message_id=%s event_id=%s", message_id, payload.event_id)
                await self._stream.ack(message_id)
                return

            image_bytes = await self._media_redis.get(redis_key)
            if not image_bytes:
                logger.info(
                    "Normalized image key is missing or expired message_id=%s event_id=%s redis_key=%s",
                    message_id,
                    payload.event_id,
                    redis_key,
                )
                await self._stream.ack(message_id)
                return

            extraction = await self._extraction_client.extract_embedding(
                detection_type=detection_type,
                image_bytes=image_bytes,
                filename=str(media_info.get("filename") or "normalized.jpg"),
                content_type=str(media_info.get("content_type") or "image/jpeg"),
            )
            resolution = self._identities.resolve(detection_type, extraction.embedding)
            await self._update_hot_state(
                payload,
                extraction,
                resolution.identity.identity_id,
                detection_type,
                resolution.is_new,
            )

            identity_threshold = self._identity_threshold(detection_type)
            observed_ts = self._observed_timestamp(payload)
            observation = IdentityObservation(
                source_event_id=payload.event_id,
                identity_id=resolution.identity.identity_id,
                detection_type=detection_type,
                is_new_identity=resolution.is_new,
                new_face=detection_type == "face" and resolution.is_new,
                new_body=detection_type == "silhouette" and resolution.is_new,
                match_confidence=resolution.confidence,
                identity_threshold=identity_threshold,
                extraction_confidence=extraction.confidence,
                embedding={
                    "kind": "face" if detection_type == "face" else "body",
                    "model": self._embedding_model(detection_type),
                    "source": "last",
                    "value": extraction.embedding,
                },
                camera_id=payload.camera_id,
                event_timestamp=payload.event_timestamp,
                event_epoch=observed_ts,
                track_id=payload.track_id,
                bbox=payload.bbox,
                age=extraction.age if extraction.age is not None else payload.age,
                gender=extraction.gender or payload.gender,
                media=payload.media,
                raw_detection=payload.model_dump(mode="json"),
            )
            observation_stream_id = await self._stream.append_identity_observation(observation)
            await self._stream.ack(message_id)

            logger.info(
                "Processed detection event_id=%s identity_id=%s detection_type=%s is_new=%s match_confidence=%s observation_stream_id=%s",
                payload.event_id,
                resolution.identity.identity_id,
                detection_type,
                resolution.is_new,
                resolution.confidence,
                observation_stream_id,
            )
        except Exception:
            logger.exception("Failed to process detection message_id=%s", message_id)

    def _parse_detection_payload(self, fields: dict[str, str]) -> DetectionPayload:
        payload = fields.get("payload")
        if not payload:
            raise ValueError("Stream message does not contain payload field")
        return DetectionPayload.model_validate_json(payload)

    def _normalized_media_info(self, media: dict[str, Any]) -> dict[str, Any]:
        field = settings.normalized_media_field
        candidates = (field, field.removeprefix("multipart:"), f"multipart:{field}")
        for candidate in candidates:
            value = media.get(candidate)
            if isinstance(value, dict):
                return value
        return {}

    def _detect_type(self, payload: DetectionPayload) -> str:
        if isinstance(payload.detector_params, dict):
            track_objects = payload.detector_params.get("track_objects")
            if isinstance(track_objects, list):
                normalized_objects = {str(item).lower() for item in track_objects}
                if {"body", "silhouette", "person"} & normalized_objects:
                    return "silhouette"
                if "face" in normalized_objects:
                    return "face"

        raw = payload.raw or {}
        labels = payload.labels or {}
        for source in (raw, labels, payload.model_extra or {}):
            for key in ("detection_type", "object_type", "label_object_type", "type"):
                value = source.get(key) if isinstance(source, dict) else None
                if value:
                    normalized = str(value).lower()
                    if "sil" in normalized or "body" in normalized or "person" in normalized:
                        return "silhouette"
                    if "face" in normalized:
                        return "face"
        if payload.silhouette_id:
            return "silhouette"
        return "face"

    def _identity_threshold(self, detection_type: str) -> float:
        if detection_type == "face":
            return settings.face_identity_confidence_threshold
        return settings.silhouette_identity_confidence_threshold

    def _embedding_model(self, detection_type: str) -> str:
        if detection_type == "face":
            return settings.face_embedding_field
        return settings.silhouette_embedding_field

    async def _update_hot_state(
        self,
        payload: DetectionPayload,
        extraction: Any,
        identity_id: str,
        detection_type: str,
        is_new_identity: bool,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        observed_ts = self._observed_timestamp(payload)
        key = f"identity:{identity_id}"
        previous_seen_raw = await self._hot_state_redis.hget(key, "last_seen_epoch")
        total_presence_seconds = await self._presence_total_seconds(key, previous_seen_raw, observed_ts)
        media_links = await self._media_links(key, payload)
        camera_ids = await self._append_unique_json_values(
            key,
            "camera_ids",
            [payload.camera_id] if payload.camera_id else [],
        )
        category_ids = await self._append_unique_json_values(key, "category_ids", self._category_ids(payload))

        mapping = {
            "identity_id": identity_id,
            "detection_type": detection_type,
            "new_face": "true" if detection_type == "face" and is_new_identity else "false",
            "new_body": "true" if detection_type == "silhouette" and is_new_identity else "false",
            "camera_id": payload.camera_id or "",
            "last_camera_id": payload.camera_id or "",
            "camera_ids": json.dumps(camera_ids, ensure_ascii=True),
            "category_ids": json.dumps(category_ids, ensure_ascii=True),
            "track_id": payload.track_id or "",
            "source_event_id": payload.event_id,
            "event_timestamp": payload.event_timestamp or "",
            "bbox": json.dumps(payload.bbox, ensure_ascii=True) if payload.bbox is not None else "",
            "extraction_confidence": "" if extraction.confidence is None else str(extraction.confidence),
            "age": "" if extraction.age is None else str(extraction.age),
            "gender": extraction.gender or payload.gender or "",
            "body_age": extraction.body_age or "",
            "body_gender": extraction.body_gender or "",
            "updated_at": now,
            "last_seen_epoch": str(observed_ts),
            "presence_total_seconds": str(total_presence_seconds),
            "media_links": json.dumps(media_links, ensure_ascii=True),
        }
        exists = await self._hot_state_redis.exists(key)
        if not exists:
            mapping["first_seen_at"] = now
            mapping["first_seen_epoch"] = str(observed_ts)
        mapping["last_seen_at"] = now

        await self._hot_state_redis.hset(key, mapping=mapping)
        await self._hot_state_redis.expire(key, settings.identity_ttl_seconds)
        await self._update_camera_state(payload, extraction, identity_id, detection_type, observed_ts, now)
        await self._update_category_state(payload, identity_id, detection_type, observed_ts, now)

    async def _update_camera_state(
        self,
        payload: DetectionPayload,
        extraction: Any,
        identity_id: str,
        detection_type: str,
        observed_ts: float,
        now: str,
    ) -> None:
        if not payload.camera_id:
            return

        key = f"identity_camera:{identity_id}:{payload.camera_id}"
        previous_seen_raw = await self._hot_state_redis.hget(key, "last_seen_epoch")
        total_presence_seconds = await self._presence_total_seconds(key, previous_seen_raw, observed_ts)
        mapping = {
            "identity_id": identity_id,
            "detection_type": detection_type,
            "camera_id": payload.camera_id,
            "track_id": payload.track_id or "",
            "source_event_id": payload.event_id,
            "event_timestamp": payload.event_timestamp or "",
            "bbox": json.dumps(payload.bbox, ensure_ascii=True) if payload.bbox is not None else "",
            "extraction_confidence": "" if extraction.confidence is None else str(extraction.confidence),
            "updated_at": now,
            "last_seen_epoch": str(observed_ts),
            "presence_total_seconds": str(total_presence_seconds),
            "last_seen_at": now,
        }
        exists = await self._hot_state_redis.exists(key)
        if not exists:
            mapping["first_seen_at"] = now
            mapping["first_seen_epoch"] = str(observed_ts)

        await self._hot_state_redis.hset(key, mapping=mapping)
        await self._hot_state_redis.expire(key, settings.identity_ttl_seconds)

    async def _update_category_state(
        self,
        payload: DetectionPayload,
        identity_id: str,
        detection_type: str,
        observed_ts: float,
        now: str,
    ) -> None:
        for category_id in self._category_ids(payload):
            key = f"identity_category:{identity_id}:{category_id}"
            camera_ids = await self._append_unique_json_values(
                key,
                "camera_ids",
                [payload.camera_id] if payload.camera_id else [],
            )
            previous_seen_raw = await self._hot_state_redis.hget(key, "last_seen_epoch")
            total_presence_seconds = await self._presence_total_seconds(key, previous_seen_raw, observed_ts)
            mapping = {
                "identity_id": identity_id,
                "detection_type": detection_type,
                "category_id": category_id,
                "last_camera_id": payload.camera_id or "",
                "camera_ids": json.dumps(camera_ids, ensure_ascii=True),
                "source_event_id": payload.event_id,
                "event_timestamp": payload.event_timestamp or "",
                "updated_at": now,
                "last_seen_epoch": str(observed_ts),
                "presence_total_seconds": str(total_presence_seconds),
                "last_seen_at": now,
            }
            exists = await self._hot_state_redis.exists(key)
            if not exists:
                mapping["first_seen_at"] = now
                mapping["first_seen_epoch"] = str(observed_ts)

            await self._hot_state_redis.hset(key, mapping=mapping)
            await self._hot_state_redis.expire(key, settings.identity_ttl_seconds)

    async def _media_links(self, key: str, payload: DetectionPayload) -> list[dict[str, Any]]:
        existing_raw = await self._hot_state_redis.hget(key, "media_links")
        try:
            existing = json.loads(existing_raw) if existing_raw else []
        except json.JSONDecodeError:
            existing = []
        if not isinstance(existing, list):
            existing = []

        links_by_url: dict[str, dict[str, Any]] = {}
        for item in existing:
            if isinstance(item, dict):
                url = item.get("public_url") or item.get("upload_url")
                if url:
                    links_by_url[str(url)] = item

        for field, media_info in (payload.media or {}).items():
            if not isinstance(media_info, dict):
                continue
            public_url = media_info.get("public_url")
            upload_url = media_info.get("upload_url")
            if not public_url and not upload_url:
                continue

            link = {
                "field": str(field),
                "media_type": str(media_info.get("media_type") or field),
                "public_url": str(public_url or upload_url),
                "upload_url": str(upload_url or public_url),
                "filename": str(media_info.get("filename") or ""),
                "content_type": str(media_info.get("content_type") or ""),
                "camera_id": payload.camera_id or "",
                "source_event_id": payload.event_id,
                "event_timestamp": payload.event_timestamp or "",
            }
            links_by_url[link["public_url"]] = link

        links = list(links_by_url.values())
        return links

    async def _append_unique_json_values(self, key: str, field: str, values: list[str]) -> list[str]:
        existing_raw = await self._hot_state_redis.hget(key, field)
        try:
            existing = json.loads(existing_raw) if existing_raw else []
        except json.JSONDecodeError:
            existing = []
        if not isinstance(existing, list):
            existing = []

        result: list[str] = []
        seen: set[str] = set()
        for value in [*existing, *values]:
            normalized = str(value).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _category_ids(self, payload: DetectionPayload) -> list[str]:
        values: list[Any] = []
        for source in (payload.labels, payload.raw, payload.model_extra):
            if not isinstance(source, dict):
                continue
            for key in (
                "category_id",
                "category_ids",
                "product_category_id",
                "product_category_ids",
                "category",
                "categories",
            ):
                if key in source:
                    values.append(source[key])
        return self._flatten_ids(values)

    def _flatten_ids(self, values: list[Any]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            items = value if isinstance(value, list | tuple | set) else [value]
            for item in items:
                if isinstance(item, dict):
                    item = item.get("id") or item.get("category_id") or item.get("name")
                normalized = str(item).strip() if item is not None else ""
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    result.append(normalized)
        return result

    def _observed_timestamp(self, payload: DetectionPayload) -> float:
        if payload.event_timestamp:
            try:
                value = payload.event_timestamp.replace("Z", "+00:00")
                return datetime.fromisoformat(value).timestamp()
            except ValueError:
                pass
        return time()

    async def _presence_total_seconds(self, key: str, previous_seen_raw: Any, observed_ts: float) -> float:
        previous_total_raw = await self._hot_state_redis.hget(key, "presence_total_seconds")
        try:
            previous_total = float(previous_total_raw or 0.0)
        except (TypeError, ValueError):
            previous_total = 0.0

        try:
            previous_seen = float(previous_seen_raw) if previous_seen_raw is not None else None
        except (TypeError, ValueError):
            previous_seen = None

        if previous_seen is None:
            return previous_total

        delta = max(0.0, observed_ts - previous_seen)
        if delta <= settings.presence_gap_seconds:
            return previous_total + delta
        return previous_total


async def main() -> None:
    configure_logging()

    stream_redis = Redis.from_url(settings.redis_stream_url, decode_responses=True)
    media_redis = Redis.from_url(settings.redis_stream_url, decode_responses=False)
    hot_state_redis = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    extraction_client = ExtractionClient(settings)

    try:
        await stream_redis.ping()
        await hot_state_redis.ping()
        processor = VisitorStateProcessor(stream_redis, media_redis, hot_state_redis, extraction_client)
        await processor.run()
    finally:
        await extraction_client.aclose()
        await stream_redis.aclose()
        await media_redis.aclose()
        await hot_state_redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
