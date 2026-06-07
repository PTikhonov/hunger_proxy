from __future__ import annotations

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import Any
from uuid import uuid4

import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.extraction_client import ExtractionClient, FaceNotFoundError
from app.similarity import cosine_similarity


logger = logging.getLogger(settings.app_name)


@dataclass
class PendingFace:
    message_id: str
    observation: dict[str, Any]
    face_embedding: list[float]
    event_epoch: float
    ready_at: float


@dataclass(frozen=True)
class CandidateFace:
    embedding: list[float]
    quality: float


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


class StreamClient:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._empty_reads = 0

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                settings.identity_observations_stream,
                settings.identity_observations_consumer_group,
                id="$",
                mkstream=True,
            )
            logger.info(
                "Created consumer group stream=%s group=%s",
                settings.identity_observations_stream,
                settings.identity_observations_consumer_group,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(self) -> list[tuple[str, dict[str, str]]]:
        pending = await self._claim_pending()
        if pending:
            logger.info(
                "Read identity observation messages source=pending stream=%s group=%s consumer=%s count=%s ids=%s",
                settings.identity_observations_stream,
                settings.identity_observations_consumer_group,
                settings.identity_observations_consumer_name,
                len(pending),
                _message_ids(pending),
            )
            return pending

        response = await self._redis.xreadgroup(
            settings.identity_observations_consumer_group,
            settings.identity_observations_consumer_name,
            streams={settings.identity_observations_stream: ">"},
            count=settings.batch_size,
            block=settings.block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _, stream_messages in response:
            for message_id, fields in stream_messages:
                messages.append((str(message_id), fields))
        if messages:
            self._empty_reads = 0
            logger.info(
                "Read identity observation messages source=new stream=%s group=%s consumer=%s count=%s ids=%s",
                settings.identity_observations_stream,
                settings.identity_observations_consumer_group,
                settings.identity_observations_consumer_name,
                len(messages),
                _message_ids(messages),
            )
        else:
            self._empty_reads += 1
            if self._empty_reads == 1 or self._empty_reads % 30 == 0:
                logger.debug(
                    "No identity observation messages stream=%s group=%s consumer=%s empty_reads=%s",
                    settings.identity_observations_stream,
                    settings.identity_observations_consumer_group,
                    settings.identity_observations_consumer_name,
                    self._empty_reads,
                )
        return messages

    async def _claim_pending(self) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            settings.identity_observations_stream,
            settings.identity_observations_consumer_group,
            settings.identity_observations_consumer_name,
            min_idle_time=settings.pending_idle_ms,
            start_id="0-0",
            count=settings.batch_size,
        )
        claimed = response[1] if len(response) > 1 else []
        return [(str(message_id), fields) for message_id, fields in claimed]

    async def ack(self, message_id: str) -> None:
        await self._redis.xack(
            settings.identity_observations_stream,
            settings.identity_observations_consumer_group,
            message_id,
        )

    async def append_person_event(self, fields: dict[str, str]) -> str:
        stream_id = await self._redis.xadd(
            settings.person_events_stream,
            fields,
            maxlen=settings.stream_maxlen,
            approximate=True,
        )
        return str(stream_id)


class MatcherService:
    def __init__(
        self,
        stream_redis: Redis,
        media_redis: Redis,
        hot_state_redis: Redis,
        extraction_client: ExtractionClient,
    ) -> None:
        self._stream = StreamClient(stream_redis)
        self._media_redis = media_redis
        self._hot_state_redis = hot_state_redis
        self._extraction_client = extraction_client
        self._http = httpx.AsyncClient(timeout=settings.extraction_timeout_seconds)
        self._pending_faces: dict[str, PendingFace] = {}

    async def aclose(self) -> None:
        await self._http.aclose()

    async def run(self) -> None:
        await self._stream.ensure_group()
        logger.info(
            "Started %s input=%s group=%s hot_state=%s extraction_api=%s",
            settings.app_name,
            settings.identity_observations_stream,
            settings.identity_observations_consumer_group,
            settings.redis_hot_state_url,
            settings.extraction_api_url,
        )
        while True:
            messages = await self._stream.read()
            for message_id, fields in messages:
                await self._handle_message(message_id, fields)
            await self._process_ready_faces()

    async def _handle_message(self, message_id: str, fields: dict[str, str]) -> None:
        try:
            observation = self._parse_observation(fields)
            detection_type = str(observation.get("detection_type") or fields.get("detection_type") or "")

            if detection_type == "silhouette":
                logger.debug(
                    "Indexing silhouette observation message_id=%s identity_id=%s camera_id=%s new_body=%s",
                    message_id,
                    observation.get("identity_id"),
                    observation.get("camera_id"),
                    observation.get("new_body"),
                )
                await self._index_silhouette_observation(message_id, observation)
                await self._stream.ack(message_id)
                return

            if detection_type != "face":
                logger.debug("Skipping observation message_id=%s detection_type=%s", message_id, detection_type)
                await self._stream.ack(message_id)
                return

            if not _bool(observation.get("new_face") if "new_face" in observation else fields.get("new_face")):
                logger.debug(
                    "Skipping face observation because new_face=false message_id=%s identity_id=%s camera_id=%s",
                    message_id,
                    observation.get("identity_id"),
                    observation.get("camera_id"),
                )
                await self._stream.ack(message_id)
                return

            face_embedding = _embedding_from_observation(observation)
            if not face_embedding:
                logger.info("New face observation has no embedding message_id=%s", message_id)
                await self._stream.ack(message_id)
                return

            event_epoch = _event_epoch(observation)
            if event_epoch is None:
                logger.info("New face observation has invalid event_timestamp message_id=%s", message_id)
                await self._stream.ack(message_id)
                return

            self._pending_faces[message_id] = PendingFace(
                message_id=message_id,
                observation=observation,
                face_embedding=face_embedding,
                event_epoch=event_epoch,
                ready_at=time() + settings.matcher_lookahead_seconds,
            )
            logger.debug(
                "Queued new face for matching message_id=%s identity_id=%s camera_id=%s event_epoch=%s ready_in_seconds=%s",
                message_id,
                observation.get("identity_id"),
                observation.get("camera_id"),
                event_epoch,
                settings.matcher_lookahead_seconds,
            )
        except Exception:
            logger.exception("Failed to handle observation message_id=%s", message_id)

    def _parse_observation(self, fields: dict[str, str]) -> dict[str, Any]:
        payload = fields.get("payload")
        if not payload:
            return dict(fields)
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise ValueError("Observation payload must be an object")
        return parsed

    async def _index_silhouette_observation(self, message_id: str, observation: dict[str, Any]) -> None:
        event_epoch = _event_epoch(observation)
        camera_id = str(observation.get("camera_id") or "")
        identity_id = str(observation.get("identity_id") or "")
        if event_epoch is None or not camera_id or not identity_id:
            return

        media_info = _normalized_media_info(observation.get("media"))
        observation_key = f"matcher_observation:{message_id}"
        mapping = {
            "observation_id": message_id,
            "payload": json.dumps(observation, ensure_ascii=True),
            "identity_id": identity_id,
            "detection_type": "silhouette",
            "new_body": "true" if _bool(observation.get("new_body")) else "false",
            "camera_id": camera_id,
            "event_epoch": str(event_epoch),
            "event_timestamp": str(observation.get("event_timestamp") or ""),
            "source_event_id": str(observation.get("source_event_id") or ""),
            "normalized_redis_key": str(media_info.get("redis_key") or ""),
            "normalized_public_url": str(media_info.get("public_url") or media_info.get("upload_url") or ""),
            "normalized_content_type": str(media_info.get("content_type") or "image/jpeg"),
            "normalized_filename": str(media_info.get("filename") or "normalized.jpg"),
        }
        index_key = f"matcher_camera_observations:{camera_id}"
        await self._hot_state_redis.hset(observation_key, mapping=mapping)
        await self._hot_state_redis.expire(observation_key, settings.matcher_observation_ttl_seconds)
        await self._hot_state_redis.zadd(index_key, {message_id: event_epoch})
        await self._hot_state_redis.expire(index_key, settings.matcher_observation_ttl_seconds)

    async def _process_ready_faces(self) -> None:
        now = time()
        ready = [item for item in self._pending_faces.values() if item.ready_at <= now]
        for pending in ready:
            try:
                await self._match_face(pending)
                await self._stream.ack(pending.message_id)
            except Exception:
                logger.exception("Failed to match face message_id=%s", pending.message_id)
            finally:
                self._pending_faces.pop(pending.message_id, None)

    async def _match_face(self, pending: PendingFace) -> None:
        face_identity_id = str(pending.observation.get("identity_id") or "")
        camera_id = str(pending.observation.get("camera_id") or "")
        if not face_identity_id or not camera_id:
            return

        candidate_ids = await self._candidate_ids(camera_id, pending.event_epoch)
        if not candidate_ids:
            logger.info("No silhouette candidates face_identity_id=%s camera_id=%s", face_identity_id, camera_id)
            return

        candidates = await self._load_candidates(candidate_ids, pending.event_epoch)
        logger.debug(
            "Loaded silhouette candidates for face face_identity_id=%s camera_id=%s count=%s window=[-%ss,+%ss]",
            face_identity_id,
            camera_id,
            len(candidates),
            settings.matcher_lookback_seconds,
            settings.matcher_lookahead_seconds,
        )
        for candidate in candidates:
            candidate_face = await self._face_embedding_from_silhouette(pending, candidate)
            if not candidate_face:
                continue

            similarity = cosine_similarity(pending.face_embedding, candidate_face.embedding)
            logger.debug(
                "Compared face with silhouette face_identity_id=%s silhouette_identity_id=%s "
                "face_observation_id=%s silhouette_observation_id=%s camera_id=%s "
                "quality=%.6f quality_threshold=%.6f similarity=%.6f similarity_threshold=%.6f time_distance=%s",
                face_identity_id,
                candidate.get("identity_id"),
                pending.message_id,
                candidate.get("observation_id"),
                camera_id,
                candidate_face.quality,
                settings.matcher_face_detection_quality_threshold,
                similarity,
                settings.matcher_face_body_confidence_threshold,
                candidate.get("time_distance"),
            )
            if similarity < settings.matcher_face_body_confidence_threshold:
                logger.debug(
                    "Face and silhouette not matched face_identity_id=%s silhouette_identity_id=%s "
                    "quality=%.6f similarity=%.6f threshold=%.6f",
                    face_identity_id,
                    candidate.get("identity_id"),
                    candidate_face.quality,
                    similarity,
                    settings.matcher_face_body_confidence_threshold,
                )
                continue

            person_id = await self._write_match(pending, candidate, similarity)
            logger.info(
                "Matched face_identity_id=%s silhouette_identity_id=%s person_id=%s similarity=%.6f quality=%.6f",
                face_identity_id,
                candidate.get("identity_id"),
                person_id,
                similarity,
                candidate_face.quality,
            )
            return

        logger.info(
            "No matching silhouette passed threshold face_identity_id=%s candidates=%s",
            face_identity_id,
            len(candidates),
        )

    async def _candidate_ids(self, camera_id: str, event_epoch: float) -> list[str]:
        index_key = f"matcher_camera_observations:{camera_id}"
        start = event_epoch - settings.matcher_lookback_seconds
        stop = event_epoch + settings.matcher_lookahead_seconds
        values = await self._hot_state_redis.zrangebyscore(index_key, start, stop)
        return [str(item) for item in values]

    async def _load_candidates(self, candidate_ids: list[str], face_epoch: float) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        for candidate_id in candidate_ids:
            fields = await self._hot_state_redis.hgetall(f"matcher_observation:{candidate_id}")
            if not fields:
                continue
            try:
                event_epoch = float(fields.get("event_epoch") or 0.0)
            except ValueError:
                continue
            fields["time_distance"] = str(abs(event_epoch - face_epoch))
            candidates.append(fields)
        candidates.sort(key=lambda item: float(item.get("time_distance") or 0.0))
        return candidates

    async def _face_embedding_from_silhouette(
        self,
        pending: PendingFace,
        candidate: dict[str, str],
    ) -> CandidateFace | None:
        image_bytes = await self._load_candidate_image(candidate)
        if not image_bytes:
            logger.debug(
                "Silhouette candidate has no normalized image face_observation_id=%s silhouette_observation_id=%s "
                "silhouette_identity_id=%s",
                pending.message_id,
                candidate.get("observation_id"),
                candidate.get("identity_id"),
            )
            return None

        try:
            extraction = await self._extraction_client.extract_face_embedding(
                image_bytes=image_bytes,
                filename=candidate.get("normalized_filename") or "normalized.jpg",
                content_type=candidate.get("normalized_content_type") or "image/jpeg",
            )
        except FaceNotFoundError:
            logger.debug(
                "Face not found in silhouette candidate face_observation_id=%s silhouette_observation_id=%s "
                "silhouette_identity_id=%s camera_id=%s",
                pending.message_id,
                candidate.get("observation_id"),
                candidate.get("identity_id"),
                candidate.get("camera_id"),
            )
            return None
        except ValueError as exc:
            logger.debug(
                "Face extraction did not produce usable embedding face_observation_id=%s silhouette_observation_id=%s "
                "silhouette_identity_id=%s camera_id=%s error=%s",
                pending.message_id,
                candidate.get("observation_id"),
                candidate.get("identity_id"),
                candidate.get("camera_id"),
                exc,
            )
            return None
        except Exception:
            logger.debug(
                "Failed to extract face from silhouette face_observation_id=%s silhouette_observation_id=%s "
                "silhouette_identity_id=%s",
                pending.message_id,
                candidate.get("observation_id"),
                candidate.get("identity_id"),
                exc_info=True,
            )
            return None

        quality = float(extraction.confidence or 0.0)
        if quality < settings.matcher_face_detection_quality_threshold:
            logger.debug(
                "Face from silhouette below quality threshold face_observation_id=%s silhouette_observation_id=%s "
                "silhouette_identity_id=%s quality=%.6f threshold=%.6f",
                pending.message_id,
                candidate.get("observation_id"),
                candidate.get("identity_id"),
                quality,
                settings.matcher_face_detection_quality_threshold,
            )
            return None

        return CandidateFace(embedding=extraction.embedding, quality=quality)

    async def _load_candidate_image(self, candidate: dict[str, str]) -> bytes | None:
        redis_key = candidate.get("normalized_redis_key")
        if redis_key:
            data = await self._media_redis.get(redis_key)
            if data:
                return data

        public_url = candidate.get("normalized_public_url")
        if not public_url:
            return None
        try:
            response = await self._http.get(public_url)
            response.raise_for_status()
            return response.content
        except Exception:
            logger.debug("Failed to load normalized image url=%s", public_url)
            return None

    async def _write_match(self, pending: PendingFace, candidate: dict[str, str], similarity: float) -> str:
        now = datetime.now(timezone.utc).isoformat()
        face_identity_id = str(pending.observation.get("identity_id") or "")
        silhouette_identity_id = str(candidate.get("identity_id") or "")
        camera_id = str(pending.observation.get("camera_id") or "")

        face_key = f"identity:{face_identity_id}"
        silhouette_key = f"identity:{silhouette_identity_id}"
        face_fields = await self._hot_state_redis.hgetall(face_key)
        silhouette_fields = await self._hot_state_redis.hgetall(silhouette_key)
        face_person_id = str(face_fields.get("person_id") or "")
        silhouette_person_id = str(silhouette_fields.get("person_id") or "")
        person_id, person_reason = _select_person_id(face_person_id, silhouette_person_id)
        person_key = person_id
        person_fields = await self._hot_state_redis.hgetall(person_key)

        face_identity_ids = _append_unique(
            _json_string_list(person_fields.get("face_identity_ids")),
            person_fields.get("face_identity_id"),
            face_identity_id,
        )
        silhouette_identity_ids = _append_unique(
            _json_string_list(person_fields.get("silhouette_identity_ids")),
            person_fields.get("silhouette_identity_id"),
            silhouette_identity_id,
        )
        matched_body_identity_ids = _append_unique(
            _json_string_list(face_fields.get("matched_body_identity_ids")),
            face_fields.get("matched_body_identity_id"),
            silhouette_identity_id,
        )
        matched_face_identity_ids = _append_unique(
            _json_string_list(silhouette_fields.get("matched_face_identity_ids")),
            silhouette_fields.get("matched_face_identity_id"),
            face_identity_id,
        )

        if face_person_id and silhouette_person_id and face_person_id != silhouette_person_id:
            logger.warning(
                "Matched identities already have different person_id values face_identity_id=%s face_person_id=%s "
                "silhouette_identity_id=%s silhouette_person_id=%s selected_person_id=%s action=no_person_merge",
                face_identity_id,
                face_person_id,
                silhouette_identity_id,
                silhouette_person_id,
                person_id,
            )

        face_mapping = {
            "person_id": person_id,
            "matched_body_identity_id": silhouette_identity_id,
            "matched_body_identity_ids": json.dumps(matched_body_identity_ids, ensure_ascii=True),
            "person_match_confidence": str(similarity),
            "person_matched_at": now,
        }
        if person_reason == "matched_existing_silhouette_person" and face_person_id != person_id:
            face_mapping["person_merge_reason"] = person_reason

        silhouette_mapping = {
            "person_id": person_id,
            "matched_face_identity_id": face_identity_id,
            "matched_face_identity_ids": json.dumps(matched_face_identity_ids, ensure_ascii=True),
            "person_match_confidence": str(similarity),
            "person_matched_at": now,
        }
        person_mapping = {
            "person_id": person_id,
            "face_identity_id": face_identity_ids[0] if face_identity_ids else face_identity_id,
            "silhouette_identity_id": silhouette_identity_ids[0] if silhouette_identity_ids else silhouette_identity_id,
            "primary_face_identity_id": face_identity_ids[0] if face_identity_ids else face_identity_id,
            "primary_silhouette_identity_id": silhouette_identity_ids[0] if silhouette_identity_ids else silhouette_identity_id,
            "last_face_identity_id": face_identity_id,
            "last_silhouette_identity_id": silhouette_identity_id,
            "face_identity_ids": json.dumps(face_identity_ids, ensure_ascii=True),
            "silhouette_identity_ids": json.dumps(silhouette_identity_ids, ensure_ascii=True),
            "camera_id": camera_id,
            "linked_at": person_fields.get("linked_at") or now,
            "updated_at": now,
            "match_confidence": str(similarity),
            "last_match_confidence": str(similarity),
            "face_observation_id": person_fields.get("face_observation_id") or pending.message_id,
            "body_observation_id": person_fields.get("body_observation_id") or str(candidate.get("observation_id") or ""),
            "last_face_observation_id": pending.message_id,
            "last_body_observation_id": str(candidate.get("observation_id") or ""),
            "face_source_event_id": person_fields.get("face_source_event_id")
            or str(pending.observation.get("source_event_id") or ""),
            "body_source_event_id": person_fields.get("body_source_event_id") or str(candidate.get("source_event_id") or ""),
            "last_face_source_event_id": str(pending.observation.get("source_event_id") or ""),
            "last_body_source_event_id": str(candidate.get("source_event_id") or ""),
        }

        await self._hot_state_redis.hset(face_key, mapping=face_mapping)
        await self._hot_state_redis.hset(silhouette_key, mapping=silhouette_mapping)
        await self._hot_state_redis.hset(person_key, mapping=person_mapping)
        await self._hot_state_redis.expire(person_key, settings.person_ttl_seconds)

        event_payload = {
            "event_type": "person_matched",
            "person_id": person_id,
            "face_identity_id": face_identity_id,
            "silhouette_identity_id": silhouette_identity_id,
            "face_identity_ids": face_identity_ids,
            "silhouette_identity_ids": silhouette_identity_ids,
            "camera_id": camera_id,
            "match_confidence": similarity,
            "person_match_reason": person_reason,
            "linked_at": now,
        }
        await self._stream.append_person_event(
            {
                "event_type": "person_matched",
                "person_id": person_id,
                "face_identity_id": face_identity_id,
                "silhouette_identity_id": silhouette_identity_id,
                "camera_id": camera_id,
                "match_confidence": str(similarity),
                "person_match_reason": person_reason,
                "payload": json.dumps(event_payload, ensure_ascii=True),
            }
        )
        logger.debug(
            "Updated person links person_id=%s reason=%s face_identity_ids=%s silhouette_identity_ids=%s",
            person_id,
            person_reason,
            face_identity_ids,
            silhouette_identity_ids,
        )
        return person_id


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"1", "true", "yes", "on"}


def _message_ids(messages: list[tuple[str, dict[str, str]]]) -> str:
    ids = [message_id for message_id, _ in messages[:10]]
    suffix = "" if len(messages) <= 10 else f"...(+{len(messages) - 10})"
    return ",".join(ids) + suffix


def _select_person_id(face_person_id: str, silhouette_person_id: str) -> tuple[str, str]:
    if silhouette_person_id:
        if face_person_id and face_person_id != silhouette_person_id:
            return silhouette_person_id, "person_conflict_selected_silhouette_person"
        return silhouette_person_id, "matched_existing_silhouette_person"
    if face_person_id:
        return face_person_id, "matched_existing_face_person"
    return f"person:{uuid4()}", "created_new_person"


def _json_string_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item)]


def _append_unique(values: list[str], *items: str | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in [*values, *items]:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _embedding_from_observation(observation: dict[str, Any]) -> list[float]:
    embedding = observation.get("embedding")
    if not isinstance(embedding, dict):
        return []
    value = embedding.get("value")
    if not isinstance(value, list):
        return []
    return [float(item) for item in value]


def _event_epoch(observation: dict[str, Any]) -> float | None:
    value = observation.get("event_epoch")
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

    timestamp = observation.get("event_timestamp")
    if not timestamp:
        return None
    try:
        return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _normalized_media_info(media: Any) -> dict[str, Any]:
    if not isinstance(media, dict):
        return {}
    for key in ("normalized", "multipart:normalized"):
        value = media.get(key)
        if isinstance(value, dict):
            return value
    for value in media.values():
        if isinstance(value, dict) and str(value.get("media_type") or "").lower() == "normalized":
            return value
    return {}


async def main() -> None:
    configure_logging()

    stream_redis = Redis.from_url(settings.redis_stream_url, decode_responses=True)
    media_redis = Redis.from_url(settings.redis_stream_url, decode_responses=False)
    hot_state_redis = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    extraction_client = ExtractionClient(settings)
    service = MatcherService(stream_redis, media_redis, hot_state_redis, extraction_client)

    try:
        await stream_redis.ping()
        await hot_state_redis.ping()
        await service.run()
    finally:
        await service.aclose()
        await extraction_client.aclose()
        await stream_redis.aclose()
        await media_redis.aclose()
        await hot_state_redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
