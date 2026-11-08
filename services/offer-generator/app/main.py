from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings


logger = logging.getLogger(settings.app_name)


PUBLISH_OFFER_SCRIPT = """
local created = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if not created then return 0 end
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[2])
redis.call('XADD', ARGV[4], 'MAXLEN', '~', ARGV[5], '*',
  'event_type', 'offer_generated', 'offer_id', ARGV[6], 'payload', ARGV[3])
return 1
"""


class InvalidThresholdEvent(ValueError):
    pass


class RulesRevisionUnavailable(RuntimeError):
    pass


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )


class OfferGenerator:
    def __init__(self, hot: Redis, rules: Redis) -> None:
        self.hot = hot
        self.rules = rules

    async def run(self) -> None:
        await self._ensure_group()
        logger.info("Started input=%s group=%s output=%s", settings.input_stream, settings.consumer_group, settings.output_stream)
        while True:
            for message_id, fields in await self._read():
                try:
                    generated = await self._process(message_id, fields)
                except (InvalidThresholdEvent, RulesRevisionUnavailable) as exc:
                    await self._dead_letter_and_ack(message_id, fields, str(exc))
                except Exception:
                    logger.exception("Failed threshold event message_id=%s", message_id)
                else:
                    await self.hot.xack(settings.input_stream, settings.consumer_group, message_id)
                    logger.info("Threshold event handled message_id=%s offer_generated=%s", message_id, generated)

    async def _ensure_group(self) -> None:
        try:
            await self.hot.xgroup_create(settings.input_stream, settings.consumer_group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.hot.xautoclaim(
            settings.input_stream, settings.consumer_group, settings.consumer_name,
            min_idle_time=settings.pending_idle_ms, start_id="0-0", count=settings.batch_size,
        )
        if len(claimed) > 1 and claimed[1]:
            return [(str(message_id), fields) for message_id, fields in claimed[1]]
        response = await self.hot.xreadgroup(
            settings.consumer_group, settings.consumer_name,
            streams={settings.input_stream: ">"}, count=settings.batch_size, block=settings.block_ms,
        )
        return [(str(message_id), fields) for _, messages in response for message_id, fields in messages]

    async def _process(self, message_id: str, fields: dict[str, str]) -> bool:
        event = _payload(fields)
        if event.get("event_type") != "interest_threshold_crossed":
            raise InvalidThresholdEvent("expected event_type=interest_threshold_crossed")
        person_id = _required_string(event, "person_id")
        location_id = _required_positive_int(event, "location_id")
        category_id = _required_positive_int(event, "matched_category_id")
        interest_level = _required_positive_int(event, "interest_lvl")
        threshold = _required_number(event, "threshold_seconds")
        revision = _required_positive_int(event, "rule_revision")

        person = await self.hot.hgetall(person_id)
        if not person:
            raise InvalidThresholdEvent(f"person state is absent: {person_id}")
        rules_payload = await self._rules_payload(location_id, revision)
        promotion = _select_promotion(rules_payload, person, category_id, interest_level)
        if promotion is None:
            return False

        context_id = promotion["context_id"]
        actions = promotion["actions"]
        cooldown = _promotion_cooldown_seconds(rules_payload, actions)
        ttl = max(settings.offer_min_dedupe_seconds, cooldown)
        offer_id = _offer_id(person_id, location_id, category_id, context_id, threshold, revision)
        offer = {
            "schema_version": 1,
            "event_type": "offer_generated",
            "offer_id": offer_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "person_id": person_id,
            "location_id": location_id,
            "camera_id": str(event.get("camera_id") or ""),
            "category_id": category_id,
            "interest": {
                "level": interest_level,
                "threshold_seconds": threshold,
                "previous_total_seconds": _number_or_none(event.get("previous_total_seconds")),
                "total_seconds": _number_or_none(event.get("total_seconds")),
            },
            "demographics": _demographics(person),
            "promotion": {
                "context_id": context_id,
                "priority": promotion["priority"],
                "rules_revision": revision,
            },
            "cooldown_seconds": cooldown,
            "actions": [_offer_action(action, rules_payload) for action in actions],
            "source_threshold_event_id": message_id,
            "source_event_id": str(event.get("source_event_id") or ""),
        }
        payload = json.dumps(offer, ensure_ascii=True, separators=(",", ":"))
        dedupe_key = f"{settings.offer_dedupe_prefix}:{person_id}:{location_id}:{category_id}:{context_id}:{_number_label(threshold)}:{revision}"
        published = await self.hot.eval(
            PUBLISH_OFFER_SCRIPT, 2,
            dedupe_key, f"{settings.offer_state_prefix}:{offer_id}",
            message_id, ttl, payload, settings.output_stream, settings.output_maxlen, offer_id,
        )
        return bool(int(published))

    async def _rules_payload(self, location_id: int, revision: int) -> dict[str, Any]:
        historical = await self.rules.get(
            f"{settings.promotion_rules_revision_key_prefix}:{location_id}:revision:{revision}"
        )
        if historical:
            return _rules_json(historical, location_id, revision)
        # Compatibility for rules received before revisioned snapshots were introduced.
        current = await self.rules.hget(f"{settings.promotion_rules_key_prefix}:{location_id}", "payload")
        if current:
            return _rules_json(current, location_id, revision)
        raise RulesRevisionUnavailable(f"promotion rules unavailable location_id={location_id} revision={revision}")

    async def _dead_letter_and_ack(self, message_id: str, fields: dict[str, str], reason: str) -> None:
        await self.hot.xadd(
            settings.dead_letter_stream,
            {
                "event_type": "offer_generation_discarded",
                "source_stream": settings.input_stream,
                "source_message_id": message_id,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "payload": fields.get("payload") or "",
            },
            maxlen=settings.dead_letter_maxlen,
            approximate=True,
        )
        await self.hot.xack(settings.input_stream, settings.consumer_group, message_id)
        logger.warning("Discarded threshold event message_id=%s reason=%s", message_id, reason)


def _payload(fields: dict[str, str]) -> dict[str, Any]:
    try:
        payload = json.loads(fields.get("payload") or "{}")
    except json.JSONDecodeError as exc:
        raise InvalidThresholdEvent("payload is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise InvalidThresholdEvent("payload must be an object")
    return payload


def _rules_json(raw: str, location_id: int, revision: int) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise RulesRevisionUnavailable(f"promotion rules JSON is invalid location_id={location_id} revision={revision}") from exc
    if not isinstance(payload, dict) or int(payload.get("revision") or 0) != revision:
        raise RulesRevisionUnavailable(f"promotion rules revision mismatch location_id={location_id} revision={revision}")
    return payload


def _select_promotion(
    payload: dict[str, Any], person: dict[str, str], category_id: int, interest_level: int,
) -> dict[str, Any] | None:
    grouped: dict[int, dict[str, Any]] = {}
    for rule in payload.get("rules") or []:
        if not isinstance(rule, dict) or not _rule_matches(rule, person, category_id, interest_level):
            continue
        context_id = _int_or_none(rule.get("context_id"))
        action = rule.get("action")
        if context_id is None or not isinstance(action, dict):
            continue
        candidate = grouped.setdefault(context_id, {
            "context_id": context_id,
            "priority": int(_int_or_none(rule.get("priority")) or 0),
            "actions": [],
        })
        candidate["actions"].append(action)
    if not grouped:
        return None
    # Priority resolves a real conflict only: several different promotions
    # already matched the same person, category and interest level.  A single
    # matching context is selected regardless of its priority value.
    if len(grouped) == 1:
        return next(iter(grouped.values()))
    return sorted(grouped.values(), key=lambda item: (-item["priority"], item["context_id"]))[0]


def _rule_matches(rule: dict[str, Any], person: dict[str, str], category_id: int, interest_level: int) -> bool:
    configured_category = rule.get("category_id")
    if configured_category not in (None, category_id):
        return False
    if _int_or_none(rule.get("interest_lvl")) != interest_level:
        return False
    person_gender = person.get("face_gender") or person.get("silhouette_gender") or person.get("gender") or ""
    if not _gender_matches(str(rule.get("gender") or "ALL"), person_gender):
        return False
    age_range = rule.get("age_range")
    if not isinstance(age_range, dict):
        return False
    return _age_matches(person, _int_or_none(age_range.get("from_age")), _int_or_none(age_range.get("to_age")))


def _gender_matches(rule_gender: str, person_gender: str) -> bool:
    desired = rule_gender.strip().lower()
    if desired in {"", "all", "any", "*"}:
        return True
    actual = person_gender.strip().lower()
    aliases = {"m": "male", "man": "male", "м": "male", "f": "female", "woman": "female", "ж": "female"}
    return aliases.get(desired, desired) == aliases.get(actual, actual) and bool(actual)


def _age_matches(person: dict[str, str], lower: int | None, upper: int | None) -> bool:
    if lower is None or upper is None:
        return False
    # Face age is authoritative. A silhouette bucket is used only when face age is absent.
    face_age = _number_or_none(person.get("face_age"))
    if face_age is not None:
        return lower <= face_age <= upper
    bucket = _age_bucket(person.get("silhouette_age") or person.get("body_age") or "")
    return bucket is not None and lower <= bucket[0] and bucket[1] <= upper


def _age_bucket(value: str) -> tuple[int, int] | None:
    value = value.strip().replace(" ", "")
    if value.endswith("+") and value[:-1].isdigit():
        return int(value[:-1]), 100
    for separator in ("-", "–", "—"):
        if separator in value:
            left, right = value.split(separator, 1)
            if left.isdigit() and right.isdigit():
                return int(left), int(right)
    return None


def _promotion_cooldown_seconds(payload: dict[str, Any], actions: list[dict[str, Any]]) -> int:
    cooldowns = payload.get("cooldowns_seconds") if isinstance(payload.get("cooldowns_seconds"), dict) else {}
    values = [_number_or_none(cooldowns.get(_cooldown_kind(action))) or 0 for action in actions]
    return max(1, *(int(value) for value in values))


def _cooldown_kind(action: dict[str, Any]) -> str:
    action_type = str(action.get("type") or "").lower()
    if "telegram" in action_type or "телеграм" in action_type:
        return "telegram"
    if "audio" in action_type or "ауди" in action_type:
        return "audio"
    return "video"


def _offer_action(action: dict[str, Any], rules_payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(action)
    result["action_id"] = action.get("id")
    result["delivery_type"] = _delivery_type(action)
    cooldowns = rules_payload.get("cooldowns_seconds")
    cooldowns = cooldowns if isinstance(cooldowns, dict) else {}
    result["cooldown_seconds"] = max(1, int(_number_or_none(cooldowns.get(_cooldown_kind(action))) or 0))
    return result


def _delivery_type(action: dict[str, Any]) -> str:
    action_type = str(action.get("type") or "").lower()
    if "telegram" in action_type or "телеграм" in action_type:
        return "telegram"
    if "audio" in action_type or "ауди" in action_type:
        return "xibo_audio"
    if "video" in action_type or "видео" in action_type:
        return "xibo_video"
    if "script" in action_type or "касс" in action_type:
        return "cashier_script"
    if "ai" in action_type or "консульт" in action_type:
        return "ai_consultant"
    return f"custom:{action.get('type_id') or 'unknown'}"


def _demographics(person: dict[str, str]) -> dict[str, Any]:
    face_age = _number_or_none(person.get("face_age"))
    silhouette_age = person.get("silhouette_age") or person.get("body_age") or None
    return {
        "age": face_age if face_age is not None else silhouette_age,
        "age_source": "face" if face_age is not None else ("silhouette" if silhouette_age else None),
        "gender": person.get("face_gender") or person.get("silhouette_gender") or person.get("gender") or None,
        "gender_source": "face" if person.get("face_gender") else ("silhouette" if person.get("silhouette_gender") else None),
    }


def _offer_id(person_id: str, location_id: int, category_id: int, context_id: int, threshold: float, revision: int) -> str:
    raw = f"{person_id}|{location_id}|{category_id}|{context_id}|{_number_label(threshold)}|{revision}"
    return "offer:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _required_string(payload: dict[str, Any], name: str) -> str:
    value = str(payload.get(name) or "")
    if not value:
        raise InvalidThresholdEvent(f"{name} is required")
    return value


def _required_positive_int(payload: dict[str, Any], name: str) -> int:
    value = _int_or_none(payload.get(name))
    if value is None or value <= 0:
        raise InvalidThresholdEvent(f"{name} must be a positive integer")
    return value


def _required_number(payload: dict[str, Any], name: str) -> float:
    value = _number_or_none(payload.get(name))
    if value is None or value <= 0:
        raise InvalidThresholdEvent(f"{name} must be positive")
    return float(value)


def _int_or_none(value: Any) -> int | None:
    number = _number_or_none(value)
    return int(number) if number is not None else None


def _number_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _number_label(value: float) -> str:
    return format(value, "g")


async def main() -> None:
    configure_logging()
    hot = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    rules = Redis.from_url(settings.redis_rules_url, decode_responses=True)
    try:
        await asyncio.gather(hot.ping(), rules.ping())
        await OfferGenerator(hot, rules).run()
    finally:
        await hot.aclose()
        await rules.aclose()


if __name__ == "__main__":
    asyncio.run(main())
