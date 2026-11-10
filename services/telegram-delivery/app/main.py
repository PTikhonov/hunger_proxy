from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings


logger = logging.getLogger(settings.app_name)


class InvalidOffer(ValueError):
    pass


class TelegramDelivery:
    def __init__(self, redis: Redis, rules: Redis, http: httpx.AsyncClient) -> None:
        self.redis = redis
        self.rules = rules
        self.http = http

    async def run(self) -> None:
        if not settings.enabled:
            logger.warning("Telegram delivery is disabled; set TELEGRAM_DELIVERY_ENABLED=true to enable it")
            while True:
                await asyncio.sleep(3600)
        await self._ensure_group()
        logger.info("Started input=%s group=%s", settings.offers_stream, settings.consumer_group)
        while True:
            for message_id, fields in await self._read():
                try:
                    delivered = await self._process(message_id, fields)
                except InvalidOffer as exc:
                    await self._dead_letter_and_ack(message_id, fields, str(exc))
                except Exception:
                    logger.exception("Telegram delivery failed message_id=%s", message_id)
                else:
                    await self.redis.xack(settings.offers_stream, settings.consumer_group, message_id)
                    logger.info("Offer handled message_id=%s telegram_actions_delivered=%s", message_id, delivered)

    async def _ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(settings.offers_stream, settings.consumer_group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.redis.xautoclaim(
            settings.offers_stream, settings.consumer_group, settings.consumer_name,
            min_idle_time=settings.pending_idle_ms, start_id="0-0", count=settings.batch_size,
        )
        if len(claimed) > 1 and claimed[1]:
            return [(str(message_id), fields) for message_id, fields in claimed[1]]
        response = await self.redis.xreadgroup(
            settings.consumer_group, settings.consumer_name,
            streams={settings.offers_stream: ">"}, count=settings.batch_size, block=settings.block_ms,
        )
        return [(str(message_id), fields) for _, messages in response for message_id, fields in messages]

    async def _process(self, message_id: str, fields: dict[str, str]) -> int:
        offer = _payload(fields)
        offer_id = _required_string(offer, "offer_id")
        actions = offer.get("actions")
        if not isinstance(actions, list):
            raise InvalidOffer("actions must be a list")
        telegram_actions = [
            action for action in actions
            if isinstance(action, dict) and action.get("delivery_type") == "telegram"
        ]
        if not telegram_actions:
            return 0
        telegram = await self._telegram_settings(offer)
        delivered = 0
        for action in telegram_actions:
            try:
                sent = await self._deliver_action(offer_id, action, telegram, offer.get("action_state"))
            except InvalidOffer as exc:
                await self._dead_letter_action(message_id, offer, action, str(exc))
                continue
            delivered += int(sent)
        return delivered

    async def _telegram_settings(self, offer: dict[str, Any]) -> dict[str, str]:
        location_id = _required_positive_int(offer, "location_id")
        promotion = offer.get("promotion")
        if not isinstance(promotion, dict):
            raise InvalidOffer("promotion must be an object")
        revision = _required_positive_int(promotion, "rules_revision")
        payload = await self.rules.get(
            f"{settings.promotion_rules_revision_key_prefix}:{location_id}:revision:{revision}"
        )
        if not payload:
            # Compatibility with snapshots written before revision history was added.
            payload = await self.rules.hget(f"{settings.promotion_rules_key_prefix}:{location_id}", "payload")
        if not payload:
            raise InvalidOffer(f"promotion rules unavailable location_id={location_id} revision={revision}")
        try:
            rules = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise InvalidOffer("promotion rules payload is not valid JSON") from exc
        if not isinstance(rules, dict) or int(rules.get("revision") or 0) != revision:
            raise InvalidOffer(f"promotion rules revision is unavailable location_id={location_id} revision={revision}")
        delivery = rules.get("delivery")
        telegram = delivery.get("telegram") if isinstance(delivery, dict) else None
        if not isinstance(telegram, dict):
            raise InvalidOffer(f"Telegram is not configured for location_id={location_id}")
        return {
            "bot_token": _required_string(telegram, "bot_token"),
            "chat_id": _required_string(telegram, "chat_id"),
        }

    async def _deliver_action(
        self, offer_id: str, action: dict[str, Any], telegram: dict[str, str], action_state: Any,
    ) -> bool:
        action_id = _required_action_id(action)
        chat_id = telegram["chat_id"]
        text = _required_string(action, "content")
        sent_key = f"{settings.sent_key_prefix}:{offer_id}:{action_id}"
        if await self.redis.exists(sent_key):
            return False

        cooldown = max(1, _positive_int(action.get("channel_cooldown_seconds"), 1))
        cooldown_key = f"{settings.cooldown_key_prefix}:{chat_id}"
        reserved = await self.redis.set(cooldown_key, offer_id, nx=True, ex=cooldown)
        if not reserved:
            logger.info("Telegram action skipped by chat cooldown offer_id=%s action_id=%s chat_id=%s", offer_id, action_id, chat_id)
            await self._clear_pending(action_state, offer_id)
            return False
        try:
            response = await self.http.post(
                f"{settings.api_base_url.rstrip('/')}/bot{telegram['bot_token']}/sendMessage",
                json={"chat_id": chat_id, "text": text},
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("ok") is not True:
                raise RuntimeError(f"Telegram API returned an unsuccessful response: {str(body)[:500]}")
        except Exception:
            # The reservation belongs to this attempted offer, so a retry can proceed.
            if await self.redis.get(cooldown_key) == offer_id:
                await self.redis.delete(cooldown_key)
            raise

        await self.redis.set(
            sent_key,
            json.dumps({"sent_at": datetime.now(timezone.utc).isoformat(), "chat_id": chat_id}),
            ex=settings.sent_ttl_seconds,
        )
        await self._mark_personal_cooldown(action_state, offer_id)
        return True

    async def _clear_pending(self, action_state: Any, offer_id: str) -> None:
        if not isinstance(action_state, dict):
            return
        pending_key = str(action_state.get("pending_key") or "")
        if pending_key and await self.redis.get(pending_key) == offer_id:
            await self.redis.delete(pending_key)

    async def _mark_personal_cooldown(self, action_state: Any, offer_id: str) -> None:
        if not isinstance(action_state, dict):
            return
        cooldown_key = str(action_state.get("cooldown_key") or "")
        pending_key = str(action_state.get("pending_key") or "")
        seconds = _positive_int(action_state.get("personal_cooldown_seconds"), 1)
        if cooldown_key:
            await self.redis.set(
                cooldown_key,
                json.dumps({"offer_id": offer_id, "delivered_at": datetime.now(timezone.utc).isoformat()}),
                ex=seconds,
            )
        if pending_key and await self.redis.get(pending_key) == offer_id:
            await self.redis.delete(pending_key)

    async def _dead_letter_action(
        self, message_id: str, offer: dict[str, Any], action: dict[str, Any], reason: str,
    ) -> None:
        await self.redis.xadd(
            settings.dead_letter_stream,
            {
                "event_type": "telegram_action_discarded",
                "source_stream": settings.offers_stream,
                "source_message_id": message_id,
                "offer_id": str(offer.get("offer_id") or ""),
                "action_id": str(action.get("action_id") or action.get("id") or ""),
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            },
            maxlen=settings.dead_letter_maxlen,
            approximate=True,
        )

    async def _dead_letter_and_ack(self, message_id: str, fields: dict[str, str], reason: str) -> None:
        await self.redis.xadd(
            settings.dead_letter_stream,
            {
                "event_type": "telegram_offer_discarded",
                "source_stream": settings.offers_stream,
                "source_message_id": message_id,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "payload": fields.get("payload") or "",
            },
            maxlen=settings.dead_letter_maxlen,
            approximate=True,
        )
        await self.redis.xack(settings.offers_stream, settings.consumer_group, message_id)
        logger.warning("Discarded offer message_id=%s reason=%s", message_id, reason)


def _payload(fields: dict[str, str]) -> dict[str, Any]:
    try:
        value = json.loads(fields.get("payload") or "{}")
    except json.JSONDecodeError as exc:
        raise InvalidOffer("payload is not valid JSON") from exc
    if not isinstance(value, dict):
        raise InvalidOffer("payload must be an object")
    return value


def _required_string(data: dict[str, Any], name: str) -> str:
    value = str(data.get(name) or "").strip()
    if not value:
        raise InvalidOffer(f"{name} is required")
    return value


def _required_action_id(action: dict[str, Any]) -> str:
    value = action.get("action_id", action.get("id"))
    if value in (None, ""):
        raise InvalidOffer("action_id is required")
    return str(value)


def _required_positive_int(data: dict[str, Any], name: str) -> int:
    try:
        value = int(data.get(name))
    except (TypeError, ValueError) as exc:
        raise InvalidOffer(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise InvalidOffer(f"{name} must be a positive integer")
    return value


def _positive_int(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # The Bot API token is embedded in the request path; never emit it through
    # verbose client logs even when the service itself uses DEBUG logging.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    redis = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    rules = Redis.from_url(settings.redis_rules_url, decode_responses=True)
    http = httpx.AsyncClient(timeout=settings.timeout_seconds)
    try:
        await redis.ping()
        await rules.ping()
        await TelegramDelivery(redis, rules, http).run()
    finally:
        await http.aclose()
        await redis.aclose()
        await rules.aclose()


if __name__ == "__main__":
    asyncio.run(main())
