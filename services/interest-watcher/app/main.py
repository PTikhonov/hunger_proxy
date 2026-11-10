from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from time import time
from typing import Any

import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.extraction import ExtractionClient, HeadNotDetectedError
from app.geometry import gaze_vector, head_point, project_direction, project_point, ray_intersects_shelf, viewing_side


logger = logging.getLogger(settings.app_name)


class UnrecoverableMediaError(RuntimeError):
    pass


class UnrecoverableObservationError(RuntimeError):
    pass

UPDATE_INTEREST_SCRIPT = """
local key = KEYS[1]
local prefix = ARGV[1]
local observed = tonumber(ARGV[2])
local intersects = ARGV[3]
local gap = tonumber(ARGV[4])
local now_iso = ARGV[5]
local detail = ARGV[6]
local ttl = tonumber(ARGV[7])
local misses_before_reset = tonumber(ARGV[8])
local total_field = prefix .. ':total_seconds'
local last_field = prefix .. ':last_observed_epoch'
local active_field = prefix .. ':intersects'
local misses_field = prefix .. ':consecutive_misses'
local total = tonumber(redis.call('HGET', key, total_field) or '0')
local previous = tonumber(redis.call('HGET', key, last_field) or '')
local was_active = redis.call('HGET', key, active_field) == 'true'
local misses = tonumber(redis.call('HGET', key, misses_field) or '0')
local active = was_active
local delta = 0
if intersects == 'true' then
  if previous and was_active then
    delta = observed - previous
    if delta >= 0 and delta <= gap then total = total + delta end
  end
  active = true
  misses = 0
else
  misses = math.min(misses + 1, misses_before_reset)
  if misses >= misses_before_reset then active = false end
end
redis.call('HSET', key,
  total_field, tostring(total),
  last_field, tostring(observed),
  active_field, active and 'true' or 'false',
  misses_field, tostring(misses),
  prefix .. ':updated_at', now_iso,
  prefix .. ':detail', detail)
redis.call('EXPIRE', key, ttl)
return {tostring(total), tostring(delta or 0)}
"""


UPDATE_CATEGORY_INTEREST_SCRIPT = """
local key = KEYS[1]
local camera_prefix = ARGV[1]
local location_category_prefix = ARGV[2]
local observed = tonumber(ARGV[3])
local now_iso = ARGV[4]
local detail = ARGV[5]
local ttl = tonumber(ARGV[6])
local camera_total_field = camera_prefix .. ':total_seconds'
local camera_last_field = camera_prefix .. ':last_observed_epoch'
local camera_active_field = camera_prefix .. ':intersects'
local category_total_field = location_category_prefix .. ':total_seconds'
local camera_total = tonumber(redis.call('HGET', key, camera_total_field) or '0')
local category_total = tonumber(redis.call('HGET', key, category_total_field) or '0')
local previous_category_total = category_total
local previous = tonumber(redis.call('HGET', key, camera_last_field) or '')
local was_active = redis.call('HGET', key, camera_active_field) == 'true'
local delta = 0
if previous and was_active then
  delta = observed - previous
  if delta < 0 or delta > tonumber(ARGV[7]) then delta = 0 end
end
camera_total = camera_total + delta
category_total = category_total + delta
redis.call('HSET', key,
  camera_total_field, tostring(camera_total),
  camera_last_field, tostring(observed),
  camera_active_field, 'true',
  camera_prefix .. ':updated_at', now_iso,
  camera_prefix .. ':detail', detail,
  category_total_field, tostring(category_total),
  location_category_prefix .. ':updated_at', now_iso,
  location_category_prefix .. ':detail', detail)
redis.call('EXPIRE', key, ttl)

local triggers = cjson.decode(ARGV[10])
local detail_object = cjson.decode(detail)
local fired = 0
for _, trigger in ipairs(triggers) do
  local threshold = tonumber(trigger.threshold)
  if threshold and threshold <= category_total then
    local crossed = previous_category_total < threshold
    local evaluation_seconds = math.max(1, tonumber(trigger.evaluation_seconds) or 1)
    local evaluation_key = trigger.evaluation_key
    if crossed or redis.call('SET', evaluation_key, now_iso, 'NX', 'EX', evaluation_seconds) then
      local event = {
        event_type = crossed and 'interest_threshold_crossed' or 'interest_threshold_active',
        occurred_at = now_iso,
        person_id = detail_object.person_id,
        location_id = detail_object.location_id,
        camera_id = detail_object.camera_id,
        matched_category_id = detail_object.matched_category_id,
        previous_total_seconds = previous_category_total,
        total_seconds = category_total,
        delta_seconds = delta,
        threshold_seconds = threshold,
        context_id = trigger.context_id,
        rule_id = trigger.rule_id,
        interest_lvl = trigger.interest_lvl,
        priority = trigger.priority,
        action = trigger.action,
        rule_revision = trigger.rule_revision,
        source_event_id = detail_object.source_event_id,
        observation_stream_id = detail_object.observation_stream_id
      }
      redis.call('XADD', ARGV[8], 'MAXLEN', '~', ARGV[9], '*',
        'event_type', event.event_type,
        'payload', cjson.encode(event))
      fired = fired + 1
    end
  end
end
return {tostring(camera_total), tostring(category_total), tostring(delta), tostring(fired)}
"""


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


class InterestWatcher:
    def __init__(
        self, stream: Redis, media: Redis, hot: Redis, geometry: Redis,
        extraction: ExtractionClient, http: httpx.AsyncClient,
    ) -> None:
        self.stream = stream
        self.media = media
        self.hot = hot
        self.geometry = geometry
        self.extraction = extraction
        self.http = http
        self._read_pending_next = False

    async def run(self) -> None:
        await self._ensure_group()
        logger.info("Started stream=%s group=%s", settings.observations_stream, settings.consumer_group)
        while True:
            for message_id, fields in await self._read():
                try:
                    await self._process(message_id, fields)
                except (UnrecoverableMediaError, UnrecoverableObservationError, HeadNotDetectedError) as exc:
                    await self._dead_letter_and_ack(message_id, fields, str(exc))
                except Exception as exc:
                    if await self._retry_limit_reached(message_id):
                        await self._dead_letter_and_ack(
                            message_id,
                            fields,
                            f"retry limit ({settings.max_retries}) reached: {exc}",
                        )
                    else:
                        logger.exception("Failed observation message_id=%s", message_id)
                else:
                    await self.stream.xack(settings.observations_stream, settings.consumer_group, message_id)

    async def _ensure_group(self) -> None:
        try:
            await self.stream.xgroup_create(settings.observations_stream, settings.consumer_group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self) -> list[tuple[str, dict[str, str]]]:
        if self._read_pending_next:
            pending = await self._claim_pending()
            self._read_pending_next = False
            if pending:
                return pending
        response = await self.stream.xreadgroup(
            settings.consumer_group,
            settings.consumer_name,
            streams={settings.observations_stream: ">"},
            count=settings.batch_size,
            block=settings.block_ms,
        )
        messages = [(str(message_id), fields) for _, messages in response for message_id, fields in messages]
        if messages:
            self._read_pending_next = True
            return messages
        return await self._claim_pending()

    async def _claim_pending(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.stream.xautoclaim(
            settings.observations_stream,
            settings.consumer_group,
            settings.consumer_name,
            min_idle_time=settings.pending_idle_ms,
            start_id="0-0",
            count=settings.batch_size,
        )
        return [(str(message_id), fields) for message_id, fields in claimed[1]] if len(claimed) > 1 else []

    async def _retry_limit_reached(self, message_id: str) -> bool:
        pending = await self.stream.xpending_range(
            settings.observations_stream,
            settings.consumer_group,
            min=message_id,
            max=message_id,
            count=1,
        )
        return bool(pending) and int(pending[0].get("times_delivered", 0)) >= settings.max_retries

    async def _process(self, message_id: str, fields: dict[str, str]) -> None:
        observation = _observation(fields)
        if str(observation.get("detection_type") or "") != "silhouette":
            return
        identity_id = str(observation.get("identity_id") or "")
        camera_id = str(observation.get("camera_id") or "")
        person_id = str(observation.get("person_id") or fields.get("person_id") or "")
        if not identity_id or not camera_id or not person_id:
            raise UnrecoverableObservationError("person event is missing person_id, identity_id or camera_id")
        geometry = await self._camera_geometry(camera_id)
        if geometry is None:
            return
        media_info = _normalized_media(observation.get("media"))
        image = await self._load_image(media_info)
        if not image:
            raise UnrecoverableMediaError("normalized silhouette image is unavailable in Redis and permanent storage")
        silhouette_bbox = _bbox(observation.get("bbox"))
        local_bbox, pose = await self.extraction.extract_head(
            image,
            str(media_info.get("filename") or "normalized.jpg"),
            str(media_info.get("content_type") or "image/jpeg"),
            body_width=silhouette_bbox[2] - silhouette_bbox[0],
            body_height=silhouette_bbox[3] - silhouette_bbox[1],
        )
        full_bbox = [
            silhouette_bbox[0] + local_bbox[0], silhouette_bbox[1] + local_bbox[1],
            silhouette_bbox[0] + local_bbox[2], silhouette_bbox[1] + local_bbox[3],
        ]
        origin = head_point(geometry["camera"], full_bbox, settings.head_height_meters)
        direction = gaze_vector(pose["yaw"], pose["pitch"], pose["roll"])
        observed_epoch = _event_epoch(observation)
        demographics = await self._silhouette_demographics(identity_id)
        hits: list[str] = []
        shelf_annotations: list[dict[str, Any]] = []
        category_hits: dict[int, list[dict[str, Any]]] = {}
        for shelf in geometry.get("shelves") or []:
            shelf_id = str(shelf["shelf_id"])
            intersects, point = ray_intersects_shelf(
                origin,
                direction,
                shelf["points"],
                _viewing_direction(shelf),
            )
            if intersects:
                hits.append(shelf_id)
            matched_category_id = _matched_category_id(shelf, direction) if intersects else None
            detail = {"source_event_id": observation.get("source_event_id"), "head_bbox": full_bbox,
                      "head_pose": pose, "head_point": origin, "gaze_vector": direction,
                      "intersection_point": point, "observation_stream_id": message_id,
                      "demographics": demographics}
            shelf_annotations.append({
                "shelf_id": shelf_id,
                "name": shelf.get("name"),
                "category_id": shelf.get("category_id"),
                "category_2_id": shelf.get("category_2_id"),
                "points_px": [_project_optional(geometry["camera"], item) for item in shelf["points"]],
                "intersects": intersects,
                "matched_category_id": matched_category_id,
                "intersection_px": _project_optional(geometry["camera"], point),
                "viewing_direction": _viewing_direction(shelf),
            })
            await self._update_interest(
                person_id, camera_id, shelf, observed_epoch, intersects,
                detail, matched_category_id, geometry.get("location_id"),
            )
            if matched_category_id is not None:
                category_hits.setdefault(matched_category_id, []).append(detail)
        for category_id, details in category_hits.items():
            location_id = _location_id(geometry.get("location_id"))
            if location_id is None:
                logger.warning("Skipping category interest without location_id camera_id=%s category_id=%s", camera_id, category_id)
                continue
            threshold_rules = await self._threshold_rules(person_id, location_id, category_id)
            await self._update_category_interest(
                person_id, camera_id, location_id, category_id, observed_epoch,
                details[0], threshold_rules,
            )
        await self._store_gaze_annotation(
            observation=observation,
            message_id=message_id,
            identity_id=identity_id,
            person_id=person_id,
            camera_id=camera_id,
            geometry=geometry,
            head_bbox=full_bbox,
            head_pose=pose,
            head_point_3d=origin,
            gaze_direction=direction,
            shelves=shelf_annotations,
        )
        logger.info("Calculated interest person_id=%s camera_id=%s shelves=%s", person_id, camera_id, hits)

    async def _store_gaze_annotation(
        self,
        *,
        observation: dict[str, Any],
        message_id: str,
        identity_id: str,
        person_id: str,
        camera_id: str,
        geometry: dict[str, Any],
        head_bbox: list[float],
        head_pose: dict[str, float],
        head_point_3d: tuple[float, float, float],
        gaze_direction: tuple[float, float, float],
        shelves: list[dict[str, Any]],
    ) -> None:
        source_event_id = str(observation.get("source_event_id") or "")
        if not source_event_id:
            logger.warning("Cannot store gaze annotation without source_event_id message_id=%s", message_id)
            return
        camera = geometry["camera"]
        payload = {
            "schema_version": 1,
            "source_event_id": source_event_id,
            "identity_id": identity_id,
            "person_id": person_id,
            "camera_id": camera_id,
            "geometry_revision": geometry.get("revision"),
            "event_timestamp": observation.get("event_timestamp") or observation.get("observed_at"),
            "event_epoch": observation.get("event_epoch"),
            "observation_stream_id": message_id,
            "frame_width": int(camera["frame_width"]),
            "frame_height": int(camera["frame_height"]),
            "head_bbox": head_bbox,
            "head_pose": head_pose,
            "head_point": head_point_3d,
            "head_distance_m": head_point_3d[2],
            "gaze_vector": gaze_direction,
            "gaze_direction_px": project_direction(camera, gaze_direction),
            "shelves": shelves,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.hot.set(
            f"gaze_annotation:{source_event_id}",
            json.dumps(payload, ensure_ascii=True),
            ex=settings.person_ttl_seconds,
        )

    async def _camera_geometry(self, camera_id: str) -> dict[str, Any] | None:
        cache_key = f"hunger:geometry:camera:{camera_id}"
        fields = await self.geometry.hgetall(cache_key)
        if not fields:
            logger.warning(
                "Geometry was not provided for camera_id=%s: key=%s is absent in Redis geometry cache",
                camera_id,
                cache_key,
            )
            return None
        if str(fields.get("deleted", "false")).lower() == "true":
            logger.warning(
                "Geometry is inactive for camera_id=%s: cached record is marked deleted",
                camera_id,
            )
            return None
        payload = json.loads(fields.get("payload") or "{}")
        if not isinstance(payload, dict):
            logger.error("Invalid cached geometry payload camera_id=%s key=%s", camera_id, cache_key)
            return None
        return payload

    async def _silhouette_demographics(self, identity_id: str) -> dict[str, Any]:
        age, gender, body_age, body_gender = await self.hot.hmget(
            f"identity:{identity_id}", "age", "gender", "body_age", "body_gender",
        )
        return {
            "age": _number_or_none(age),
            "gender": gender or None,
            "body_age": body_age or None,
            "body_gender": body_gender or None,
        }

    async def _load_image(self, media_info: dict[str, Any]) -> bytes | None:
        redis_key = str(media_info.get("redis_key") or "")
        if redis_key:
            image = await self.media.get(redis_key)
            if image:
                return image

        public_url = str(media_info.get("public_url") or media_info.get("upload_url") or "")
        if not public_url:
            return None
        try:
            response = await self.http.get(public_url)
        except httpx.HTTPError:
            # Network failures are transient: retain the message in PEL for retry.
            raise
        if 400 <= response.status_code < 500 and response.status_code not in {408, 429}:
            return None
        response.raise_for_status()
        return response.content or None

    async def _dead_letter_and_ack(self, message_id: str, fields: dict[str, str], reason: str) -> None:
        await self.stream.xadd(
            settings.dead_letter_stream,
            {
                "event_type": "interest_observation_discarded",
                "source_stream": settings.observations_stream,
                "source_message_id": message_id,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "payload": fields.get("payload") or "",
            },
            maxlen=settings.dead_letter_maxlen,
            approximate=True,
        )
        await self.stream.xack(settings.observations_stream, settings.consumer_group, message_id)
        logger.warning(
            "Discarded unrecoverable observation message_id=%s reason=%s dead_letter_stream=%s",
            message_id,
            reason,
            settings.dead_letter_stream,
        )

    async def _update_interest(
        self, person_id: str, camera_id: str, shelf: dict[str, Any], observed_epoch: float,
        intersects: bool, detail: dict[str, Any], matched_category_id: int | None,
        location_id: Any,
    ) -> None:
        shelf_id = str(shelf["shelf_id"])
        prefix = f"interest:{camera_id}:{shelf_id}"
        detail.update({
            "camera_id": camera_id,
            "location_id": location_id,
            "shelf_id": shelf_id,
            "shelf_name": shelf.get("name"),
            "category_id": shelf.get("category_id"),
            "category_2_id": shelf.get("category_2_id"),
            "matched_category_id": matched_category_id,
            "intersects": intersects,
        })
        await self.hot.eval(
            UPDATE_INTEREST_SCRIPT, 1, person_id, prefix, observed_epoch,
            "true" if intersects else "false", settings.interest_gap_seconds,
            datetime.now(timezone.utc).isoformat(), json.dumps(detail, ensure_ascii=True), settings.person_ttl_seconds,
            settings.interest_misses_before_reset,
        )

    async def _update_category_interest(
        self, person_id: str, camera_id: str, location_id: Any, category_id: int,
        observed_epoch: float, detail: dict[str, Any], threshold_rules: list[dict[str, Any]],
    ) -> None:
        camera_prefix = f"interest-category:{camera_id}:{category_id}"
        location_category_prefix = f"interest-category:{location_id}:{category_id}"
        category_detail = dict(detail)
        category_detail.update({
            "person_id": person_id,
            "camera_id": camera_id,
            "location_id": location_id,
            "matched_category_id": category_id,
            "intersects": True,
        })
        result = await self.hot.eval(
            UPDATE_CATEGORY_INTEREST_SCRIPT, 1, person_id,
            camera_prefix, location_category_prefix, observed_epoch,
            datetime.now(timezone.utc).isoformat(), json.dumps(category_detail, ensure_ascii=True),
            settings.person_ttl_seconds, settings.interest_gap_seconds,
            settings.threshold_events_stream, settings.threshold_events_maxlen,
            json.dumps(threshold_rules, ensure_ascii=True),
        )
        if int(result[3]) > 0:
            logger.info(
                "Published interest threshold event person_id=%s location_id=%s category_id=%s count=%s",
                person_id, location_id, category_id, result[3],
            )

    async def _threshold_rules(self, person_id: str, location_id: int, category_id: int) -> list[dict[str, Any]]:
        raw_payload = await self.geometry.hget(
            f"{settings.promotion_rules_key_prefix}:{location_id}", "payload",
        )
        if not raw_payload:
            return []
        try:
            payload = json.loads(raw_payload)
        except (TypeError, ValueError):
            logger.warning("Invalid promotion rules payload location_id=%s", location_id)
            return []
        if not isinstance(payload, dict):
            return []
        return _threshold_rules(payload, person_id, location_id, category_id)


def _observation(fields: dict[str, str]) -> dict[str, Any]:
    parsed = json.loads(fields.get("payload") or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("observation payload must be an object")
    return parsed


def _normalized_media(media: Any) -> dict[str, Any]:
    if not isinstance(media, dict):
        return {}
    for key in ("normalized", "multipart:normalized"):
        if isinstance(media.get(key), dict):
            return media[key]
    return {}


def _bbox(value: Any) -> list[float]:
    while isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
        value = value[0]
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        raise ValueError(f"invalid silhouette bbox: {value!r}")
    return [float(item) for item in value]


def _event_epoch(observation: dict[str, Any]) -> float:
    value = observation.get("event_epoch")
    if value not in (None, ""):
        return float(value)
    timestamp = str(observation.get("event_timestamp") or "")
    if timestamp:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
    return time()


def _number_or_none(value: Any) -> float | int | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _location_id(value: Any) -> int | None:
    try:
        location_id = int(value)
    except (TypeError, ValueError):
        return None
    return location_id if location_id > 0 else None


def _threshold_rules(
    payload: dict[str, Any], person_id: str, location_id: int, category_id: int,
) -> list[dict[str, Any]]:
    """Return location rules eligible to emit a category threshold event.

    Demographic selection remains the responsibility of the reaction service;
    this worker's authoritative fact is the category-interest threshold crossing.
    """
    thresholds = payload.get("thresholds_seconds")
    rules = payload.get("rules")
    if not isinstance(thresholds, dict) or not isinstance(rules, list):
        return []
    level_names = {1: "passed", 2: "attention", 3: "involved", 4: "buy"}
    revision = _number_or_none(payload.get("revision"))
    result: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        rule_category = rule.get("category_id")
        if rule_category not in (None, category_id):
            continue
        try:
            interest_level = int(rule.get("interest_lvl"))
        except (TypeError, ValueError):
            continue
        threshold = _number_or_none(thresholds.get(level_names.get(interest_level, "")))
        if threshold is None or threshold <= 0:
            continue
        action = rule.get("action") if isinstance(rule.get("action"), dict) else {}
        context_id = _number_or_none(rule.get("context_id"))
        rule_id = _number_or_none(action.get("id")) or context_id
        if context_id is None or rule_id is None:
            continue
        threshold_label = format(float(threshold), "g")
        evaluation_key = (
            f"{settings.threshold_cooldown_prefix}:{person_id}:{location_id}:{category_id}:"
            f"{interest_level}:{threshold_label}"
        )
        result.append({
            "context_id": int(context_id),
            "rule_id": int(rule_id),
            "interest_lvl": interest_level,
            "priority": int(_number_or_none(rule.get("priority")) or 0),
            "threshold": threshold,
            "evaluation_seconds": settings.offer_evaluation_interval_seconds,
            "evaluation_key": evaluation_key,
            "rule_revision": int(revision) if revision is not None else None,
            "action": action,
        })
    return result


def _matched_category_id(shelf: dict[str, Any], direction: tuple[float, float, float]) -> int | None:
    side = viewing_side(shelf["points"], direction)
    if side is None:
        return None
    for category_field, side_field in (("category_id", "side"), ("category_2_id", "side_2")):
        category_id = shelf.get(category_field)
        configured_side = str(shelf.get(side_field) or "").upper()
        if category_id is not None and side in configured_side:
            return int(category_id)
    return None


def _viewing_direction(shelf: dict[str, Any]) -> str:
    explicit = str(shelf.get("viewing_direction") or "").lower()
    if explicit in {"normal", "opposite", "both"}:
        return explicit
    # A one-sided shelf is viewed from the side opposite to its UI arrow.
    # hunger_head's A arrow is opposite to the normal operand order used here,
    # therefore A permits the normal approach and B permits the opposite one.
    sides = {str(shelf.get(name) or "").lower() for name in ("side", "side_2")}
    sides.discard("")
    if "ab" in sides or {"a", "b"}.issubset(sides):
        return "both"
    return {"a": "normal", "b": "opposite"}.get(next(iter(sides), ""), "both")


def _project_optional(camera: dict[str, Any], point: Any) -> list[float] | None:
    if point is None:
        return None
    try:
        return list(project_point(camera, point))
    except (KeyError, TypeError, ValueError):
        return None


async def main() -> None:
    configure_logging()
    stream = Redis.from_url(settings.redis_stream_url, decode_responses=True)
    media = Redis.from_url(settings.redis_stream_url, decode_responses=False)
    hot = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    geometry = Redis.from_url(settings.redis_geometry_url, decode_responses=True)
    extraction = ExtractionClient(settings)
    http = httpx.AsyncClient(timeout=settings.media_download_timeout_seconds)
    try:
        await asyncio.gather(stream.ping(), hot.ping(), geometry.ping())
        await InterestWatcher(stream, media, hot, geometry, extraction, http).run()
    finally:
        await extraction.aclose()
        await http.aclose()
        await stream.aclose()
        await media.aclose()
        await hot.aclose()
        await geometry.aclose()


if __name__ == "__main__":
    asyncio.run(main())
