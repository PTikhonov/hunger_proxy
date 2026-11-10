from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings


logger = logging.getLogger(settings.app_name)


class InvalidOffer(ValueError):
    """The offer cannot be delivered and must not be retried."""


class XiboDelivery:
    def __init__(self, redis: Redis, rules: Redis, http: httpx.AsyncClient) -> None:
        self.redis = redis
        self.rules = rules
        self.http = http
        self._token = ""
        self._token_expires_at = 0.0
        self._token_fingerprint = ""
        self._token_lock = asyncio.Lock()

    async def run(self) -> None:
        if not settings.enabled:
            logger.warning("Xibo %s delivery is disabled; set XIBO_DELIVERY_ENABLED=true to enable it", settings.delivery_label)
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
                    logger.exception("Xibo audio delivery failed message_id=%s", message_id)
                else:
                    await self.redis.xack(settings.offers_stream, settings.consumer_group, message_id)
                    logger.info("Offer handled message_id=%s xibo_%s_actions_delivered=%s", message_id, settings.delivery_label, delivered)

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
        xibo_actions = [
            action for action in actions
            if isinstance(action, dict) and action.get("delivery_type") == settings.delivery_type
        ]
        if not xibo_actions:
            return 0
        xibo = await self._xibo_settings(offer)
        delivered = 0
        for action in xibo_actions:
            try:
                sent = await self._deliver_action(offer_id, action, xibo, offer.get("action_state"))
            except InvalidOffer as exc:
                await self._dead_letter_action(message_id, offer, action, str(exc))
                continue
            delivered += int(sent)
        return delivered

    async def _xibo_settings(self, offer: dict[str, Any]) -> dict[str, str]:
        location_id = _required_positive_int(offer, "location_id")
        promotion = offer.get("promotion")
        if not isinstance(promotion, dict):
            raise InvalidOffer("promotion must be an object")
        revision = _required_positive_int(promotion, "rules_revision")
        payload = await self.rules.get(
            f"{settings.promotion_rules_revision_key_prefix}:{location_id}:revision:{revision}"
        )
        if not payload:
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
        xibo = delivery.get("xibo") if isinstance(delivery, dict) else None
        values = xibo if isinstance(xibo, dict) else {}
        cms_url = str(values.get("cms_url") or values.get("base_url") or settings.xibo_cms_url).strip()
        client_id = str(values.get("client_id") or settings.xibo_client_id).strip()
        client_secret = str(values.get("client_secret") or settings.xibo_client_secret).strip()
        if not cms_url or not client_id or not client_secret:
            raise InvalidOffer(f"Xibo is not configured for location_id={location_id}")
        return {"cms_url": cms_url.rstrip("/"), "client_id": client_id, "client_secret": client_secret}

    async def _deliver_action(
        self, offer_id: str, action: dict[str, Any], xibo: dict[str, str], action_state: Any,
    ) -> bool:
        action_id = _required_action_id(action)
        target_kind, target_id = _target(action)
        layout_id = _layout_id_from_content(action)
        sent_key = f"{settings.sent_key_prefix}:{offer_id}:{action_id}"
        if await self.redis.exists(sent_key):
            return False

        cooldown = max(1, _positive_int(action.get("channel_cooldown_seconds"), 1))
        cooldown_key = f"{settings.cooldown_key_prefix}:{target_kind}:{target_id}"
        reserved = await self.redis.set(cooldown_key, offer_id, nx=True, ex=cooldown)
        if not reserved:
            logger.info("Xibo action skipped by target cooldown offer_id=%s action_id=%s target=%s:%s", offer_id, action_id, target_kind, target_id)
            await self._clear_pending(action_state, offer_id)
            return False
        try:
            token = await self._access_token(xibo)
            response = await self.http.post(
                f"{xibo['cms_url']}/api/{target_kind}/{target_id}/action/changeLayout",
                params={"envelope": "1"},
                data={"layoutId": str(layout_id), "changeMode": "replace"},
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()
            _ensure_xibo_success(response)
        except Exception:
            if await self.redis.get(cooldown_key) == offer_id:
                await self.redis.delete(cooldown_key)
            raise

        await self.redis.set(
            sent_key,
            json.dumps({"sent_at": datetime.now(timezone.utc).isoformat(), "target": f"{target_kind}:{target_id}", "layout_id": layout_id}),
            ex=settings.sent_ttl_seconds,
        )
        await self._mark_personal_cooldown(action_state, offer_id)
        return True

    async def _access_token(self, xibo: dict[str, str]) -> str:
        fingerprint = f"{xibo['cms_url']}\x00{xibo['client_id']}\x00{xibo['client_secret']}"
        async with self._token_lock:
            if self._token and self._token_fingerprint == fingerprint and time.monotonic() < self._token_expires_at:
                return self._token
            response = await self.http.post(
                f"{xibo['cms_url']}/api/authorize/access_token",
                data={"grant_type": "client_credentials", "client_id": xibo["client_id"], "client_secret": xibo["client_secret"]},
            )
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                raise RuntimeError("Xibo authorization returned invalid JSON") from exc
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise RuntimeError("Xibo authorization response does not contain access_token")
            expires_in = _positive_int(payload.get("expires_in") if isinstance(payload, dict) else None, 60)
            self._token = token
            self._token_fingerprint = fingerprint
            self._token_expires_at = time.monotonic() + max(1, expires_in - min(30, expires_in // 2))
            return token

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

    async def _dead_letter_action(self, message_id: str, offer: dict[str, Any], action: dict[str, Any], reason: str) -> None:
        await self.redis.xadd(
            settings.dead_letter_stream,
            {
                "event_type": f"xibo_{settings.delivery_label}_action_discarded", "source_stream": settings.offers_stream,
                "source_message_id": message_id, "offer_id": str(offer.get("offer_id") or ""),
                "action_id": str(action.get("action_id") or action.get("id") or ""), "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }, maxlen=settings.dead_letter_maxlen, approximate=True,
        )

    async def _dead_letter_and_ack(self, message_id: str, fields: dict[str, str], reason: str) -> None:
        await self.redis.xadd(
            settings.dead_letter_stream,
            {
                "event_type": f"xibo_{settings.delivery_label}_offer_discarded", "source_stream": settings.offers_stream,
                "source_message_id": message_id, "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(), "payload": fields.get("payload") or "",
            }, maxlen=settings.dead_letter_maxlen, approximate=True,
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


def _target(action: dict[str, Any]) -> tuple[str, int]:
    group_id = _optional_positive_int(action.get("player_group_id"))
    player_id = _optional_positive_int(action.get("player_id"))
    if bool(group_id) == bool(player_id):
        raise InvalidOffer("exactly one of player_group_id or player_id is required")
    return ("displaygroup", group_id) if group_id else ("display", player_id)


def _optional_positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidOffer("player target id must be a positive integer") from exc
    if result <= 0:
        raise InvalidOffer("player target id must be a positive integer")
    return result


def _layout_id_from_content(action: dict[str, Any]) -> int:
    # Django selector values may be stored as "<id>&<label>"; the API requires only the id.
    raw = _required_string(action, "content").split("&", 1)[0].strip()
    try:
        layout_id = int(raw)
    except ValueError as exc:
        raise InvalidOffer("content must start with a positive Xibo layout id") from exc
    if layout_id <= 0:
        raise InvalidOffer("content must start with a positive Xibo layout id")
    return layout_id


def _ensure_xibo_success(response: httpx.Response) -> None:
    """Reject explicit CMS error envelopes while accepting version-specific success shapes."""
    try:
        body = response.json()
    except json.JSONDecodeError:
        return
    if isinstance(body, dict) and body.get("success") is False:
        raise RuntimeError(f"Xibo CMS rejected layout change: {str(body)[:500]}")


async def main() -> None:
    logging.basicConfig(level=getattr(logging, settings.log_level), format="%(asctime)s %(levelname)s [%(name)s] %(message)s", stream=sys.stdout, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    redis = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    rules = Redis.from_url(settings.redis_rules_url, decode_responses=True)
    http = httpx.AsyncClient(timeout=settings.timeout_seconds)
    try:
        await redis.ping()
        await rules.ping()
        await XiboDelivery(redis, rules, http).run()
    finally:
        await http.aclose()
        await redis.aclose()
        await rules.aclose()


if __name__ == "__main__":
    asyncio.run(main())
