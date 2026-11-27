from __future__ import annotations

import asyncio
import hmac
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from typing import Annotated, AsyncIterator

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from redis.asyncio import Redis
from redis.exceptions import ResponseError
import httpx

from app.config import settings
from app.schemas import ApplyResult, DeviceSnapshot
from app.storage import DeviceRegistry, RevisionConflictError


logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(settings.app_name)


class InvalidOffer(ValueError):
    pass


class SlideshowDelivery:
    """Consumes SlideShow actions after the project device registry is synced."""

    def __init__(self, hot: Redis, registry: DeviceRegistry, http: httpx.AsyncClient) -> None:
        self.hot = hot
        self.registry = registry
        self.http = http

    async def run(self) -> None:
        await self._ensure_group()
        logger.info("Started input=%s group=%s", settings.offers_stream, settings.consumer_group)
        while True:
            for message_id, fields in await self._read():
                try:
                    delivered = await self._process(message_id, fields)
                except InvalidOffer as exc:
                    await self._dead_letter_and_ack(message_id, fields, str(exc))
                except Exception:
                    # Do not ACK a temporary device, network or Redis error.
                    logger.exception("SlideShow delivery failed message_id=%s", message_id)
                else:
                    await self.hot.xack(settings.offers_stream, settings.consumer_group, message_id)
                    logger.info("Offer handled message_id=%s slideshow_devices_delivered=%s", message_id, delivered)

    async def _ensure_group(self) -> None:
        try:
            await self.hot.xgroup_create(settings.offers_stream, settings.consumer_group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.hot.xautoclaim(
            settings.offers_stream, settings.consumer_group, settings.consumer_name,
            min_idle_time=settings.pending_idle_ms, start_id="0-0", count=settings.batch_size,
        )
        if len(claimed) > 1 and claimed[1]:
            return [(str(message_id), fields) for message_id, fields in claimed[1]]
        response = await self.hot.xreadgroup(
            settings.consumer_group, settings.consumer_name,
            streams={settings.offers_stream: ">"}, count=settings.batch_size, block=settings.block_ms,
        )
        return [(str(message_id), fields) for _, messages in response for message_id, fields in messages]

    async def _process(self, message_id: str, fields: dict[str, str]) -> int:
        offer = _payload(fields)
        offer_id = _required_string(offer, "offer_id")
        project_id = _required_positive_int(offer, "project_id")
        actions = offer.get("actions")
        if not isinstance(actions, list):
            raise InvalidOffer("actions must be a list")
        slideshow_actions = [
            action for action in actions
            if isinstance(action, dict) and (
                action.get("delivery_type") == "slideshow"
                or "slideshow" in (action.get("delivery_types") or [])
            )
        ]
        if not slideshow_actions:
            return 0
        snapshot = await self.registry.get(project_id)
        if snapshot is None:
            raise RuntimeError(f"slideshow device registry is unavailable project_id={project_id}")
        delivered = 0
        for action in slideshow_actions:
            delivered += await self._deliver_action(offer_id, action, snapshot, offer.get("action_state"))
        return delivered

    async def _deliver_action(
        self, offer_id: str, action: dict[str, Any], snapshot: dict[str, Any], action_state: Any,
    ) -> int:
        action_id = _required_action_id(action)
        group_id = _optional_positive_int(action.get("slideshow_group_id"))
        device_id = _optional_positive_int(action.get("slideshow_device_id"))
        if group_id and device_id:
            raise InvalidOffer("choose either slideshow_group_id or slideshow_device_id")
        if not group_id and not device_id:
            raise InvalidOffer("slideshow_group_id or slideshow_device_id is required")
        content = _required_string(action, "content")
        devices = [
            device for device in snapshot.get("devices") or []
            if isinstance(device, dict)
            and device.get("enabled")
            and (
                device.get("id") == device_id
                if device_id else group_id in set(device.get("group_ids") or [])
            )
        ]
        if not devices:
            target = f"device_id={device_id}" if device_id else f"group_id={group_id}"
            raise InvalidOffer(f"no enabled SlideShow devices for {target}")
        delivered = 0
        cooldown = _positive_int(action.get("channel_cooldown_seconds"), 1)
        for device in devices:
            device_id = _required_positive_int(device, "id")
            sent_key = f"{settings.sent_key_prefix}:{offer_id}:{action_id}:{device_id}"
            if await self.hot.exists(sent_key):
                continue
            cooldown_key = f"{settings.cooldown_key_prefix}:{device_id}"
            reserved = await self.hot.set(cooldown_key, offer_id, nx=True, ex=cooldown)
            if not reserved:
                logger.info("SlideShow action skipped by device cooldown offer_id=%s device_id=%s", offer_id, device_id)
                continue
            try:
                credentials = device.get("credentials") or {}
                response = await self.http.post(
                    f"{str(device['api_url']).rstrip('/')}/ajax/showFile",
                    params={"file": content},
                    auth=(str(credentials.get("username") or ""), str(credentials.get("password") or "")),
                )
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or body.get("success") is not True:
                    raise RuntimeError(f"SlideShow API returned unsuccessful response: {str(body)[:500]}")
            except Exception:
                if await self.hot.get(cooldown_key) == offer_id:
                    await self.hot.delete(cooldown_key)
                raise
            await self.hot.set(
                sent_key,
                json.dumps({"delivered_at": datetime.now(timezone.utc).isoformat(), "device_id": device_id}),
                ex=settings.sent_ttl_seconds,
            )
            delivered += 1
        if delivered:
            await self._mark_personal_cooldown(action_state, offer_id)
        return delivered

    async def _mark_personal_cooldown(self, action_state: Any, offer_id: str) -> None:
        if not isinstance(action_state, dict):
            return
        cooldown_key = str(action_state.get("cooldown_key") or "")
        pending_key = str(action_state.get("pending_key") or "")
        seconds = _positive_int(action_state.get("personal_cooldown_seconds"), 1)
        if cooldown_key:
            await self.hot.set(
                cooldown_key,
                json.dumps({"offer_id": offer_id, "delivered_at": datetime.now(timezone.utc).isoformat()}),
                ex=seconds,
            )
        if pending_key and await self.hot.get(pending_key) == offer_id:
            await self.hot.delete(pending_key)

    async def _dead_letter_and_ack(self, message_id: str, fields: dict[str, str], reason: str) -> None:
        await self.hot.xadd(
            settings.dead_letter_stream,
            {
                "event_type": "slideshow_offer_discarded",
                "source_stream": settings.offers_stream,
                "source_message_id": message_id,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "payload": fields.get("payload") or "",
            },
            maxlen=settings.dead_letter_maxlen,
            approximate=True,
        )
        await self.hot.xack(settings.offers_stream, settings.consumer_group, message_id)
        logger.warning("Discarded offer message_id=%s reason=%s", message_id, reason)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    hot = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    config = Redis.from_url(settings.redis_config_url, decode_responses=True)
    await hot.ping()
    await config.ping()
    app.state.hot = hot
    app.state.registry = DeviceRegistry(config, settings)
    http = httpx.AsyncClient(timeout=settings.http_timeout_seconds)
    task = asyncio.create_task(SlideshowDelivery(hot, app.state.registry, http).run()) if settings.enabled else None
    try:
        yield
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await http.aclose()
        await hot.aclose()
        await config.aclose()


app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)


def require_service_token(authorization: Annotated[str | None, Header()] = None) -> None:
    if not settings.service_token:
        return
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(token, settings.service_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid service token")


def registry(request: Request) -> DeviceRegistry:
    return request.app.state.registry


RegistryDependency = Annotated[DeviceRegistry, Depends(registry)]


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    await request.app.state.hot.ping()
    return {"status": "ok", "service": settings.app_name}


@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    await request.app.state.hot.ping()
    return {"status": "ready", "service": settings.app_name}


@app.put(
    "/internal/v1/projects/{project_id}/slideshow-devices",
    response_model=ApplyResult,
    dependencies=[Depends(require_service_token)],
)
async def put_devices(project_id: int, snapshot: DeviceSnapshot, store: RegistryDependency) -> ApplyResult:
    if snapshot.project_id != project_id:
        raise HTTPException(status_code=422, detail="project_id in path and body must match")
    try:
        result = await store.apply(snapshot.model_dump(mode="json"))
    except RevisionConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "project_id": exc.project_id,
                "incoming_revision": exc.incoming_revision,
                "current_revision": exc.current_revision,
            },
        ) from exc
    return ApplyResult(status=result, project_id=project_id, revision=snapshot.revision)


@app.get(
    "/internal/v1/projects/{project_id}/slideshow-devices",
    dependencies=[Depends(require_service_token)],
)
async def get_devices(project_id: int, store: RegistryDependency) -> dict:
    payload = await store.get(project_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="slideshow device registry not found")
    return payload


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


def _required_positive_int(data: dict[str, Any], name: str) -> int:
    try:
        value = int(data.get(name))
    except (TypeError, ValueError) as exc:
        raise InvalidOffer(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise InvalidOffer(f"{name} must be a positive integer")
    return value


def _optional_positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        value = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidOffer("SlideShow target must be a positive integer") from exc
    if value <= 0:
        raise InvalidOffer("SlideShow target must be a positive integer")
    return value


def _required_action_id(action: dict[str, Any]) -> str:
    value = action.get("action_id", action.get("id"))
    if value in (None, ""):
        raise InvalidOffer("action_id is required")
    return str(value)


def _positive_int(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
