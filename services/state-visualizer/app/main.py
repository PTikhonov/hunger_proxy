from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from redis.asyncio import Redis

from app.config import settings


app = FastAPI(title=settings.app_name, version="0.1.0")
redis: Redis | None = None


@app.on_event("startup")
async def startup() -> None:
    global redis
    redis = Redis.from_url(settings.redis_hot_state_url, decode_responses=True)
    await redis.ping()


@app.on_event("shutdown")
async def shutdown() -> None:
    if redis is not None:
        await redis.aclose()


@app.get("/health")
async def health() -> dict[str, str]:
    assert redis is not None
    await redis.ping()
    return {"status": "ok", "service": settings.app_name}


@app.get("/api/state")
async def state() -> dict[str, Any]:
    assert redis is not None
    identities: list[dict[str, Any]] = []
    cameras_by_identity: dict[str, list[dict[str, Any]]] = {}

    async for key in redis.scan_iter(match="identity:*", count=settings.identity_scan_count):
        fields = await redis.hgetall(key)
        if not fields:
            continue
        ttl_seconds = await redis.ttl(key)
        identities.append(_identity_from_hash(str(key), fields, ttl_seconds))

    async for key in redis.scan_iter(match="person:*", count=settings.identity_scan_count):
        fields = await redis.hgetall(key)
        if not fields:
            continue
        ttl_seconds = await redis.ttl(key)
        identities.append(_person_from_hash(str(key), fields, ttl_seconds))

    async for key in redis.scan_iter(match="identity_camera:*", count=settings.identity_scan_count):
        fields = await redis.hgetall(key)
        if not fields:
            continue
        ttl_seconds = await redis.ttl(key)
        camera_state = _camera_state_from_hash(str(key), fields, ttl_seconds)
        cameras_by_identity.setdefault(camera_state["identity_id"], []).append(camera_state)

    for identity in identities:
        camera_states = cameras_by_identity.get(identity["identity_id"], [])
        camera_states.sort(key=lambda item: item.get("last_seen_epoch_sort", 0.0), reverse=True)
        if identity.get("detection_type") == "person":
            linked_camera_states = _linked_person_camera_states(identity, cameras_by_identity)
            identity["camera_states"] = _merge_camera_states(identity.get("camera_states", []), linked_camera_states)
        elif camera_states or not identity.get("camera_states"):
            identity["camera_states"] = camera_states

    identities.sort(key=lambda item: item.get("first_seen_epoch_sort", 0.0))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "poll_interval_ms": settings.poll_interval_ms,
        "counts": {
            "silhouette": sum(1 for item in identities if item["bucket"] == "silhouette"),
            "face": sum(1 for item in identities if item["bucket"] == "face"),
            "matched": sum(1 for item in identities if item["bucket"] == "matched"),
        },
        "identities": identities,
    }


@app.get("/api/gaze-annotation/{source_event_id}")
async def gaze_annotation(source_event_id: str) -> dict[str, Any]:
    assert redis is not None
    raw = await redis.get(f"gaze_annotation:{source_event_id}")
    if not raw:
        raise HTTPException(status_code=404, detail="gaze annotation not found")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="invalid gaze annotation") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="invalid gaze annotation")
    return payload


@app.post("/api/admin/clear-hot-state")
async def clear_hot_state(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    assert redis is not None
    delete_media = bool(payload.get("delete_media", True))
    media_stats = {
        "requested": 0,
        "deleted": 0,
        "missing": 0,
        "failed": 0,
        "failures": [],
    }
    if delete_media:
        media_urls = await _collect_media_urls()
        media_stats = await _delete_media_urls(media_urls)

    deleted = 0
    async for key in redis.scan_iter(match="*", count=settings.identity_scan_count):
        deleted += await redis.delete(key)
    return {
        "status": "ok",
        "deleted_keys": deleted,
        "media_delete_requested": delete_media,
        "media": media_stats,
        "cleared_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


async def _collect_media_urls() -> list[str]:
    assert redis is not None
    urls: set[str] = set()
    async for key in redis.scan_iter(match="identity:*", count=settings.identity_scan_count):
        media_links_raw = await redis.hget(key, "media_links")
        for item in _json_list(media_links_raw):
            url = item.get("upload_url") or item.get("public_url")
            if url:
                urls.add(str(url))
    return sorted(urls)


async def _delete_media_urls(urls: list[str]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "requested": len(urls),
        "deleted": 0,
        "missing": 0,
        "failed": 0,
        "failures": [],
    }
    if not urls:
        return stats

    semaphore = asyncio.Semaphore(settings.media_delete_concurrency)
    async with httpx.AsyncClient(timeout=settings.media_delete_timeout_seconds) as client:
        results = await asyncio.gather(*[_delete_media_url(client, semaphore, url) for url in urls])

    for result in results:
        status = result["status"]
        if status == "deleted":
            stats["deleted"] += 1
        elif status == "missing":
            stats["missing"] += 1
        else:
            stats["failed"] += 1
            if len(stats["failures"]) < 20:
                stats["failures"].append({"url": result["url"], "status_code": result.get("status_code")})
    return stats


async def _delete_media_url(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, url: str) -> dict[str, Any]:
    async with semaphore:
        try:
            response = await client.delete(url)
        except httpx.HTTPError:
            return {"url": url, "status": "failed", "status_code": None}

    if response.status_code in {200, 202, 204}:
        return {"url": url, "status": "deleted", "status_code": response.status_code}
    if response.status_code == 404:
        return {"url": url, "status": "missing", "status_code": response.status_code}
    return {"url": url, "status": "failed", "status_code": response.status_code}


def _person_from_hash(key: str, fields: dict[str, str], ttl_seconds: int) -> dict[str, Any]:
    linked_at = fields.get("linked_at") or fields.get("created_at") or fields.get("updated_at") or ""
    sort_epoch = _person_sort_epoch(fields)
    face_identity_ids = _append_unique_strings(
        _json_string_list(fields.get("face_identity_ids")),
        fields.get("face_identity_id"),
        fields.get("primary_face_identity_id"),
        fields.get("last_face_identity_id"),
    )
    silhouette_identity_ids = _append_unique_strings(
        _json_string_list(fields.get("silhouette_identity_ids")),
        fields.get("silhouette_identity_id"),
        fields.get("primary_silhouette_identity_id"),
        fields.get("last_silhouette_identity_id"),
    )

    return {
        "key": key,
        "bucket": "matched",
        "identity_id": fields.get("person_id") or key,
        "detection_type": "person",
        "person_id": fields.get("person_id") or key,
        "ttl_seconds": ttl_seconds,
        "ttl_display": _ttl_display(ttl_seconds),
        "duration_seconds": _identity_duration_seconds(fields),
        "duration_display": _identity_duration_display(fields),
        "not_seen_seconds": _not_seen_seconds(fields),
        "not_seen_display": _not_seen_display(fields),
        "first_seen_epoch_sort": sort_epoch,
        "last_seen_epoch_sort": sort_epoch,
        "media_links": [],
        "camera_states": _person_camera_states(fields, ttl_seconds),
        "interests": _interests_from_fields(fields),
        "fields": _display_fields(fields),
        "linked_face_identity_id": face_identity_ids[0] if face_identity_ids else "",
        "linked_silhouette_identity_id": silhouette_identity_ids[0] if silhouette_identity_ids else "",
        "linked_face_identity_ids": face_identity_ids,
        "linked_silhouette_identity_ids": silhouette_identity_ids,
        "linked_at": _format_iso(linked_at),
        "match_confidence": fields.get("match_confidence") or "",
        "match_status": fields.get("match_status") or "",
    }


def _identity_from_hash(key: str, fields: dict[str, str], ttl_seconds: int) -> dict[str, Any]:
    media_links = _json_list(fields.get("media_links"))
    media_links.sort(key=lambda item: str(item.get("event_timestamp") or item.get("source_event_id") or ""))
    display_fields = _display_fields(fields)

    detection_type = fields.get("detection_type") or ""
    person_id = fields.get("person_id") or fields.get("matched_person_id") or fields.get("person_identity_id") or ""
    bucket = "silhouette" if detection_type == "silhouette" else "face"

    return {
        "key": key,
        "bucket": bucket,
        "identity_id": fields.get("identity_id") or key.removeprefix("identity:"),
        "detection_type": detection_type,
        "person_id": person_id,
        "ttl_seconds": ttl_seconds,
        "ttl_display": _ttl_display(ttl_seconds),
        "duration_seconds": _identity_duration_seconds(fields),
        "duration_display": _identity_duration_display(fields),
        "not_seen_seconds": _not_seen_seconds(fields),
        "not_seen_display": _not_seen_display(fields),
        "first_seen_epoch_sort": _float_or_zero(fields.get("first_seen_epoch")),
        "last_seen_epoch_sort": _float_or_zero(fields.get("last_seen_epoch")),
        "media_links": media_links,
        "fields": display_fields,
    }


def _camera_state_from_hash(key: str, fields: dict[str, str], ttl_seconds: int) -> dict[str, Any]:
    return {
        "key": key,
        "identity_id": fields.get("identity_id") or _identity_id_from_camera_key(key),
        "camera_id": fields.get("camera_id") or _camera_id_from_camera_key(key),
        "ttl_seconds": ttl_seconds,
        "ttl_display": _ttl_display(ttl_seconds),
        "presence_total_seconds": _float_or_zero(fields.get("presence_total_seconds")),
        "presence_total_display": _seconds_display(_float_or_zero(fields.get("presence_total_seconds"))),
        "duration_seconds": _identity_duration_seconds(fields),
        "duration_display": _identity_duration_display(fields),
        "not_seen_seconds": _not_seen_seconds(fields),
        "not_seen_display": _not_seen_display(fields),
        "last_seen_epoch_sort": _float_or_zero(fields.get("last_seen_epoch")),
        "fields": _display_fields(fields),
    }


def _person_camera_states(fields: dict[str, str], ttl_seconds: int) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for state_type, raw in (
        ("face", fields.get("face_camera_states")),
        ("silhouette", fields.get("silhouette_camera_states")),
    ):
        parsed = _json_dict(raw)
        for camera_id, state_fields in parsed.items():
            if not isinstance(state_fields, dict):
                continue
            normalized = {str(key): str(value) for key, value in state_fields.items() if value is not None}
            normalized.setdefault("camera_id", str(camera_id))
            states.append(
                {
                    "key": f"person_camera:{fields.get('person_id') or ''}:{state_type}:{camera_id}",
                    "identity_id": normalized.get("identity_id") or "",
                    "camera_id": normalized.get("camera_id") or str(camera_id),
                    "state_type": state_type,
                    "ttl_seconds": ttl_seconds,
                    "ttl_display": _ttl_display(ttl_seconds),
                    "presence_total_seconds": _float_or_zero(normalized.get("presence_total_seconds")),
                    "presence_total_display": _seconds_display(_float_or_zero(normalized.get("presence_total_seconds"))),
                    "duration_seconds": _identity_duration_seconds(normalized),
                    "duration_display": _identity_duration_display(normalized),
                    "not_seen_seconds": _not_seen_seconds(normalized),
                    "not_seen_display": _not_seen_display(normalized),
                    "last_seen_epoch_sort": _float_or_zero(normalized.get("last_seen_epoch")),
                    "fields": _display_fields(normalized),
                }
            )
    states.sort(key=lambda item: item.get("last_seen_epoch_sort", 0.0), reverse=True)
    return states


def _linked_person_camera_states(
    identity: dict[str, Any],
    cameras_by_identity: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for state_type, identity_ids in (
        ("face", identity.get("linked_face_identity_ids") or []),
        ("silhouette", identity.get("linked_silhouette_identity_ids") or []),
    ):
        for identity_id in identity_ids:
            for camera_state in cameras_by_identity.get(str(identity_id), []):
                state = dict(camera_state)
                state["state_type"] = state_type
                states.append(state)
    return states


def _merge_camera_states(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for group in groups:
        for state in group:
            key = (
                str(state.get("state_type") or ""),
                str(state.get("identity_id") or ""),
                str(state.get("camera_id") or ""),
            )
            current = merged.get(key)
            if current is None or _float_or_zero(state.get("last_seen_epoch_sort")) >= _float_or_zero(
                current.get("last_seen_epoch_sort")
            ):
                merged[key] = state
    values = list(merged.values())
    values.sort(key=lambda item: item.get("last_seen_epoch_sort", 0.0), reverse=True)
    return values


def _identity_id_from_camera_key(key: str) -> str:
    parts = key.split(":")
    return parts[1] if len(parts) >= 3 else ""


def _camera_id_from_camera_key(key: str) -> str:
    parts = key.split(":")
    return parts[2] if len(parts) >= 3 else ""


def _json_list(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


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


def _json_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _append_unique_strings(values: list[str], *items: str | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in [*values, *items]:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _float_or_zero(value: str | None) -> float:
    try:
        return float(value or 0.0)
    except ValueError:
        return 0.0


def _person_sort_epoch(fields: dict[str, str]) -> float:
    for key in ("linked_at", "created_at", "updated_at"):
        value = fields.get(key)
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return 0.0


def _display_fields(fields: dict[str, str]) -> dict[str, str]:
    hidden = {"media_links"}
    return {
        key: _display_value(key, value)
        for key, value in fields.items()
        if key not in hidden and not key.startswith("interest:")
    }


def _interests_from_fields(fields: dict[str, str]) -> list[dict[str, Any]]:
    interests: list[dict[str, Any]] = []
    suffix = ":total_seconds"
    for field, value in fields.items():
        if not field.startswith("interest:") or not field.endswith(suffix):
            continue
        prefix = field.removesuffix(suffix)
        detail = _json_dict(fields.get(f"{prefix}:detail"))
        total = _float_or_zero(value)
        interests.append(
            {
                "camera_id": str(detail.get("camera_id") or ""),
                "shelf_id": str(detail.get("shelf_id") or ""),
                "shelf_name": str(detail.get("shelf_name") or ""),
                "category_id": str(detail.get("category_id") or ""),
                "intersects": str(fields.get(f"{prefix}:intersects") or "false").lower() == "true",
                "total_seconds": total,
                "total_display": _seconds_display(total),
                "updated_at": _format_iso(str(fields.get(f"{prefix}:updated_at") or "")),
            }
        )
    interests.sort(key=lambda item: (item["camera_id"], item["shelf_id"]))
    return interests


def _display_value(key: str, value: str) -> str:
    if key in {"first_seen_epoch", "last_seen_epoch"}:
        return _format_epoch(value)
    if key in {"last_seen_at", "first_seen_at", "event_timestamp", "updated_at", "linked_at"}:
        return _format_iso(value)
    return value


def _format_epoch(value: str) -> str:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "")
    except (TypeError, ValueError):
        return value


def _format_iso(value: str) -> str:
    if not value:
        return value
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).replace(microsecond=0).isoformat().replace("+00:00", "")
    except ValueError:
        return value


def _ttl_display(ttl_seconds: int) -> str:
    if ttl_seconds == -1:
        return "no TTL"
    if ttl_seconds == -2:
        return "expired"
    if ttl_seconds < 0:
        return str(ttl_seconds)
    minutes, seconds = divmod(ttl_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def _identity_duration_seconds(fields: dict[str, str]) -> float:
    first_seen = _float_or_zero(fields.get("first_seen_epoch"))
    last_seen = _float_or_zero(fields.get("last_seen_epoch"))
    if not first_seen or not last_seen:
        return 0.0
    return max(0.0, last_seen - first_seen)


def _identity_duration_display(fields: dict[str, str]) -> str:
    return _seconds_display(_identity_duration_seconds(fields))


def _not_seen_seconds(fields: dict[str, str]) -> float:
    last_seen = _float_or_zero(fields.get("last_seen_epoch"))
    if not last_seen:
        return 0.0
    return max(0.0, datetime.now(timezone.utc).timestamp() - last_seen)


def _not_seen_display(fields: dict[str, str]) -> str:
    return _seconds_display(_not_seen_seconds(fields))


def _seconds_display(value: float) -> str:
    return f"{value:.1f}s"


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>State Visualizer</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f7f9;
      --panel: #ffffff;
      --line: #d8e0e7;
      --text: #1c252c;
      --muted: #60717f;
      --sil: #256f72;
      --face: #7a4c18;
      --match: #375f9f;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
    }

    header {
      min-height: 92px;
      display: flex;
      flex-direction: column;
      align-items: stretch;
      justify-content: center;
      gap: 10px;
      padding: 12px 18px;
      border-bottom: 1px solid var(--line);
      background: #ffffff;
      position: sticky;
      top: 0;
      z-index: 2;
    }

    .top-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    h1 {
      font-size: 18px;
      margin: 0;
      font-weight: 700;
    }

    .status {
      display: flex;
      gap: 14px;
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }

    nav {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      overflow-x: auto;
    }

    .menu-item {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      height: 34px;
      padding: 0 12px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f8fafb;
      color: var(--text);
      font: inherit;
      font-size: 13px;
      white-space: nowrap;
    }

    .menu-item.danger {
      border-color: #d9b6b6;
      color: #8a2525;
      background: #fff7f7;
      cursor: pointer;
    }

    .menu-item.danger:disabled {
      opacity: 0.55;
      cursor: wait;
    }

    .icon-button {
      width: 34px;
      height: 34px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f8fafb;
      color: var(--text);
      font-size: 15px;
      line-height: 1;
      cursor: pointer;
    }

    .icon-button:hover {
      background: #eef3f6;
    }

    .menu-check {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 34px;
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
      user-select: none;
    }

    .menu-check input {
      width: 15px;
      height: 15px;
      margin: 0;
    }

    main {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      height: calc(100vh - 92px);
      min-height: 0;
      overflow: hidden;
    }

    section {
      min-width: 0;
      min-height: 0;
      padding: 14px;
      border-right: 1px solid var(--line);
      overflow-y: auto;
      overscroll-behavior: contain;
    }

    section:last-child { border-right: none; }

    .column-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      font-size: 15px;
      font-weight: 700;
    }

    .count {
      min-width: 28px;
      height: 24px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      background: #e8eef3;
      color: var(--muted);
      font-size: 12px;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid var(--face);
      border-radius: 8px;
      margin-bottom: 12px;
      overflow: hidden;
      box-shadow: 0 1px 2px rgba(28, 37, 44, 0.06);
    }

    .silhouette .card { border-left-color: var(--sil); }
    .matched .card { border-left-color: var(--match); }

    .card.highlight {
      outline: 3px solid #2f9e85;
      outline-offset: 2px;
    }

    .card-head {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 3px;
    }

    .identity {
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .meta {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }

    .jump-link {
      border: 0;
      padding: 0;
      background: transparent;
      color: #1f679a;
      font: inherit;
      font-size: 12px;
      text-align: left;
      text-decoration: underline;
      cursor: pointer;
      overflow-wrap: anywhere;
    }

    .jump-link:hover {
      color: #174b70;
    }

    .camera-states {
      display: grid;
      gap: 6px;
      padding-top: 4px;
    }

    .camera-row {
      display: grid;
      grid-template-columns: minmax(70px, 22%) minmax(0, 1fr);
      gap: 8px;
      align-items: start;
      padding: 6px 8px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f8fafb;
    }

    .camera-id {
      font-weight: 700;
      font-size: 12px;
      overflow-wrap: anywhere;
    }

    .camera-metrics {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }

    details {
      border-bottom: 1px solid var(--line);
    }

    summary {
      cursor: pointer;
      padding: 9px 12px;
      color: var(--muted);
      font-size: 12px;
      user-select: none;
    }

    .fields {
      padding: 10px 12px;
      display: grid;
      grid-template-columns: minmax(120px, 34%) minmax(0, 1fr);
      gap: 6px 8px;
    }

    .field-key {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }

    .field-value {
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      overflow-wrap: anywhere;
      white-space: pre-wrap;
    }

    .images {
      padding: 10px 12px 12px;
      display: grid;
      gap: 8px;
    }

    .image-row {
      display: grid;
      grid-template-columns: 74px minmax(0, 1fr);
      gap: 8px;
      align-items: start;
    }

    .thumb {
      width: 74px;
      height: 96px;
      object-fit: contain;
      background: #edf2f5;
      border: 1px solid var(--line);
      border-radius: 6px;
    }

    .link-meta {
      font-size: 12px;
      color: var(--muted);
      overflow-wrap: anywhere;
      display: grid;
      gap: 3px;
    }

    .gaze-modal {
      position: fixed;
      inset: 0;
      z-index: 20;
      display: none;
      grid-template-rows: auto minmax(0, 1fr);
      background: rgba(6, 12, 18, 0.9);
      color: #ffffff;
    }

    .gaze-modal.open { display: grid; }

    .gaze-modal-head {
      min-width: 0;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 10px 14px;
      background: rgba(12, 21, 29, 0.96);
      border-bottom: 1px solid rgba(255, 255, 255, 0.2);
    }

    .gaze-modal-meta {
      min-width: 0;
      display: grid;
      gap: 3px;
      font-size: 12px;
      color: #d3dde5;
      overflow-wrap: anywhere;
    }

    .gaze-modal-close {
      flex: 0 0 auto;
      width: 36px;
      height: 36px;
      border: 1px solid rgba(255, 255, 255, 0.35);
      border-radius: 6px;
      background: rgba(255, 255, 255, 0.08);
      color: #ffffff;
      font-size: 22px;
      cursor: pointer;
    }

    .gaze-modal-body {
      min-width: 0;
      min-height: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 14px;
      overflow: auto;
    }

    .gaze-frame {
      position: relative;
      display: inline-block;
      line-height: 0;
      box-shadow: 0 3px 24px rgba(0, 0, 0, 0.5);
    }

    .gaze-frame img {
      display: block;
      width: auto;
      height: auto;
      max-width: calc(100vw - 28px);
      max-height: calc(100vh - 92px);
    }

    .gaze-frame svg {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      pointer-events: none;
    }

    .overlay-stroke {
      vector-effect: non-scaling-stroke;
    }

    .empty {
      color: var(--muted);
      padding: 16px;
      border: 1px dashed var(--line);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.5);
    }

    @media (max-width: 1100px) {
      main {
        grid-template-columns: 1fr;
        overflow-y: auto;
      }
      section { border-right: none; border-bottom: 1px solid var(--line); }
    }
  </style>
</head>
<body>
  <header>
    <div class="top-row">
      <h1>state-visualizer</h1>
      <div class="status">
        <span id="updated">waiting</span>
        <span id="total">0 identities</span>
      </div>
    </div>
    <nav aria-label="Management">
      <span class="menu-item">Redis Hot State</span>
      <button class="icon-button" id="refresh-toggle" type="button" title="Pause updates" aria-label="Pause updates">▶</button>
      <label class="menu-check">
        <input id="delete-media" type="checkbox" checked />
        <span>Delete media files</span>
      </label>
      <button class="menu-item danger" id="clear-hot-state" type="button">Clear all hot state</button>
    </nav>
  </header>

  <main>
    <section class="silhouette">
      <div class="column-title"><span>Silhouettes</span><span class="count" id="count-silhouette">0</span></div>
      <div id="silhouette-list"></div>
    </section>
    <section class="face">
      <div class="column-title"><span>Faces</span><span class="count" id="count-face">0</span></div>
      <div id="face-list"></div>
    </section>
    <section class="matched">
      <div class="column-title"><span>Matched</span><span class="count" id="count-matched">0</span></div>
      <div id="matched-list"></div>
    </section>
  </main>

  <div class="gaze-modal" id="gaze-modal" role="dialog" aria-modal="true" aria-label="Full frame gaze overlay">
    <div class="gaze-modal-head">
      <div class="gaze-modal-meta">
        <strong id="gaze-modal-event"></strong>
        <span id="gaze-modal-status">loading annotation…</span>
      </div>
      <button class="gaze-modal-close" id="gaze-modal-close" type="button" aria-label="Close">×</button>
    </div>
    <div class="gaze-modal-body" id="gaze-modal-body">
      <div class="gaze-frame">
        <img id="gaze-full-frame" alt="full frame" />
        <svg id="gaze-overlay" aria-hidden="true"></svg>
      </div>
    </div>
  </div>

  <script>
    const lists = {
      silhouette: document.getElementById("silhouette-list"),
      face: document.getElementById("face-list"),
      matched: document.getElementById("matched-list"),
    };

    const counts = {
      silhouette: document.getElementById("count-silhouette"),
      face: document.getElementById("count-face"),
      matched: document.getElementById("count-matched"),
    };

    let pollMs = 1500;
    const openDetails = new Set();
    let pollTimer = null;
    let updatesPaused = false;

    const refreshToggle = document.getElementById("refresh-toggle");
    const gazeModal = document.getElementById("gaze-modal");
    const gazeModalBody = document.getElementById("gaze-modal-body");
    const gazeModalEvent = document.getElementById("gaze-modal-event");
    const gazeModalStatus = document.getElementById("gaze-modal-status");
    const gazeFullFrame = document.getElementById("gaze-full-frame");
    const gazeOverlay = document.getElementById("gaze-overlay");
    const SVG_NS = "http://www.w3.org/2000/svg";
    let gazeModalRequest = 0;
    let activeGazeAnnotation = null;

    function svgNode(name, attributes = {}) {
      const node = document.createElementNS(SVG_NS, name);
      for (const [key, value] of Object.entries(attributes)) {
        node.setAttribute(key, String(value));
      }
      return node;
    }

    function numericPair(value) {
      if (!Array.isArray(value) || value.length !== 2) return null;
      const pair = value.map(Number);
      return pair.every(Number.isFinite) ? pair : null;
    }

    function addOverlayText(x, y, value, color = "#ffffff", anchor = "start") {
      const text = svgNode("text", {
        x, y, fill: color, "font-size": 18, "font-family": "Arial, sans-serif",
        "font-weight": 700, "text-anchor": anchor, stroke: "rgba(0, 0, 0, 0.9)",
        "stroke-width": 4, "paint-order": "stroke", "stroke-linejoin": "round",
        class: "overlay-stroke",
      });
      text.textContent = value;
      gazeOverlay.appendChild(text);
    }

    function renderGazeOverlay(annotation) {
      gazeOverlay.replaceChildren();
      const width = Number(annotation.frame_width) || gazeFullFrame.naturalWidth;
      const height = Number(annotation.frame_height) || gazeFullFrame.naturalHeight;
      if (!(width > 0 && height > 0)) return;
      gazeOverlay.setAttribute("viewBox", `0 0 ${width} ${height}`);
      gazeOverlay.setAttribute("preserveAspectRatio", "none");

      const defs = svgNode("defs");
      const marker = svgNode("marker", {
        id: "gaze-arrowhead", viewBox: "0 0 10 10", refX: 9, refY: 5,
        markerWidth: 12, markerHeight: 12, orient: "auto", markerUnits: "userSpaceOnUse",
      });
      marker.appendChild(svgNode("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "#ffd43b" }));
      defs.appendChild(marker);
      gazeOverlay.appendChild(defs);

      for (const shelf of annotation.shelves || []) {
        const points = (shelf.points_px || []).map(numericPair).filter(Boolean);
        if (points.length < 3) continue;
        const hit = Boolean(shelf.intersects);
        const color = hit ? "#51cf66" : "#38d9a9";
        gazeOverlay.appendChild(svgNode("polygon", {
          points: points.map(point => point.join(",")).join(" "),
          fill: color, "fill-opacity": 0.18, stroke: color, "stroke-width": 3,
          "stroke-linejoin": "round", class: "overlay-stroke",
        }));
        const centerX = points.reduce((total, point) => total + point[0], 0) / points.length;
        const centerY = points.reduce((total, point) => total + point[1], 0) / points.length;
        addOverlayText(centerX, centerY, `Shelf ${shelf.name || shelf.shelf_id || ""}`, color, "middle");

        const intersection = numericPair(shelf.intersection_px);
        if (intersection) {
          gazeOverlay.appendChild(svgNode("circle", {
            cx: intersection[0], cy: intersection[1], r: 7,
            fill: hit ? "#51cf66" : "#ff6b6b", stroke: "#ffffff", "stroke-width": 2,
            class: "overlay-stroke",
          }));
        }
      }

      const bbox = Array.isArray(annotation.head_bbox) ? annotation.head_bbox.map(Number) : [];
      if (bbox.length !== 4 || !bbox.every(Number.isFinite)) return;
      const [left, top, right, bottom] = bbox;
      gazeOverlay.appendChild(svgNode("rect", {
        x: left, y: top, width: Math.max(0, right - left), height: Math.max(0, bottom - top),
        fill: "none", stroke: "#ffd43b", "stroke-width": 3, class: "overlay-stroke",
      }));

      const startX = left + (right - left) / 2;
      const startY = top + (bottom - top) / 2;
      const direction = numericPair(annotation.gaze_direction_px);
      if (direction) {
        const magnitude = Math.hypot(direction[0], direction[1]);
        if (magnitude > 1e-6) {
          const unitX = direction[0] / magnitude;
          const unitY = direction[1] / magnitude;
          let length = Math.min(width, height) * 0.22;
          const limits = [];
          if (unitX > 0) limits.push((width - startX) / unitX);
          if (unitX < 0) limits.push((0 - startX) / unitX);
          if (unitY > 0) limits.push((height - startY) / unitY);
          if (unitY < 0) limits.push((0 - startY) / unitY);
          if (limits.length) length = Math.max(0, Math.min(length, Math.min(...limits) - 4));
          gazeOverlay.appendChild(svgNode("line", {
            x1: startX, y1: startY, x2: startX + unitX * length, y2: startY + unitY * length,
            stroke: "#ffd43b", "stroke-width": 4, "marker-end": "url(#gaze-arrowhead)",
            class: "overlay-stroke",
          }));
        }
      }

      const distance = Number(annotation.head_distance_m);
      if (Number.isFinite(distance)) {
        const labelY = top >= 26 ? top - 9 : Math.min(height - 8, bottom + 23);
        addOverlayText(Math.max(8, left), labelY, `Head distance ${distance.toFixed(2)} m`, "#ffd43b");
      }
    }

    function updateGazeModalStatus(annotation) {
      const dimensions = `${annotation.frame_width || "?"}×${annotation.frame_height || "?"}`;
      const distance = Number(annotation.head_distance_m);
      const distanceText = Number.isFinite(distance) ? ` | head distance ${distance.toFixed(2)} m` : "";
      const mismatch = gazeFullFrame.naturalWidth && gazeFullFrame.naturalHeight &&
        (Number(annotation.frame_width) !== gazeFullFrame.naturalWidth || Number(annotation.frame_height) !== gazeFullFrame.naturalHeight)
        ? ` | image ${gazeFullFrame.naturalWidth}×${gazeFullFrame.naturalHeight}` : "";
      gazeModalStatus.textContent = `camera ${annotation.camera_id || ""} | geometry revision ${annotation.geometry_revision ?? "?"} | frame ${dimensions}${distanceText}${mismatch}`;
    }

    async function openGazeModal(sourceEventId, fullFrameUrl) {
      const requestId = ++gazeModalRequest;
      activeGazeAnnotation = null;
      gazeOverlay.replaceChildren();
      gazeModalEvent.textContent = sourceEventId || fullFrameUrl;
      gazeModalStatus.textContent = "loading annotation…";
      gazeModal.classList.add("open");
      document.body.style.overflow = "hidden";
      gazeFullFrame.onload = () => {
        if (requestId !== gazeModalRequest) return;
        if (activeGazeAnnotation) {
          renderGazeOverlay(activeGazeAnnotation);
          updateGazeModalStatus(activeGazeAnnotation);
        }
      };
      gazeFullFrame.onerror = () => {
        if (requestId === gazeModalRequest) gazeModalStatus.textContent = "failed to load full frame";
      };
      gazeFullFrame.src = fullFrameUrl;

      if (!sourceEventId) {
        gazeModalStatus.textContent = "gaze annotation unavailable: source_event_id is absent";
        return;
      }
      try {
        const response = await fetch(`/api/gaze-annotation/${encodeURIComponent(sourceEventId)}`, { cache: "no-store" });
        if (!response.ok) {
          const message = response.status === 404 ? "gaze annotation is not available for this frame" : `annotation error ${response.status}`;
          throw new Error(message);
        }
        const annotation = await response.json();
        if (requestId !== gazeModalRequest) return;
        activeGazeAnnotation = annotation;
        if (gazeFullFrame.complete && gazeFullFrame.naturalWidth) {
          renderGazeOverlay(annotation);
          updateGazeModalStatus(annotation);
        }
      } catch (error) {
        if (requestId === gazeModalRequest) gazeModalStatus.textContent = String(error.message || error);
      }
    }

    function closeGazeModal() {
      gazeModalRequest += 1;
      activeGazeAnnotation = null;
      gazeModal.classList.remove("open");
      gazeOverlay.replaceChildren();
      gazeFullFrame.removeAttribute("src");
      document.body.style.overflow = "";
    }

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[ch]));
    }

    function fieldRows(fields) {
      return Object.entries(fields)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, value]) => `
          <div class="field-key">${esc(key)}</div>
          <div class="field-value">${esc(value)}</div>
        `).join("");
    }

    function normalizedImageItems(mediaLinks) {
      const byEvent = new Map();
      for (const link of mediaLinks) {
        const eventId = link.source_event_id || link.event_timestamp || Math.random().toString();
        const item = byEvent.get(eventId) || {};
        const mediaType = String(link.media_type || link.field || "");
        if (mediaType.includes("normalized")) {
          item.normalized = link;
        } else if (mediaType.includes("full_frame") || mediaType.includes("photo")) {
          item.fullFrame = link;
        }
        item.event_timestamp = item.event_timestamp || link.event_timestamp || "";
        item.source_event_id = item.source_event_id || link.source_event_id || "";
        item.camera_id = item.camera_id || link.camera_id || "";
        byEvent.set(eventId, item);
      }

      const rows = Array.from(byEvent.values()).filter(item => item.normalized);
      rows.sort((a, b) => String(a.event_timestamp).localeCompare(String(b.event_timestamp)));
      return rows;
    }

    function imageRow(item, gazeEnabled) {
      const normalizedUrl = item.normalized.public_url || item.normalized.upload_url || "";
      const fullFrameUrl = item.fullFrame
        ? (item.fullFrame.public_url || item.fullFrame.upload_url || normalizedUrl)
        : inferFullFrameUrl(normalizedUrl);
      const fullFrameAttributes = gazeEnabled
        ? `href="${esc(fullFrameUrl)}" data-gaze-overlay data-source-event-id="${esc(item.source_event_id || "")}" data-full-frame-url="${esc(fullFrameUrl)}"`
        : `href="${esc(fullFrameUrl)}" target="_blank" rel="noreferrer"`;
      return `
        <div class="image-row">
          <a ${fullFrameAttributes}>
            <img class="thumb" src="${esc(normalizedUrl)}" loading="lazy" alt="normalized" />
          </a>
          <div class="link-meta">
            <strong>normalized</strong>
            <a ${fullFrameAttributes}>open full_frame</a>
            ${item.camera_id ? `<span>camera ${esc(item.camera_id)}</span>` : ""}
            <span>${esc(item.event_timestamp || "")}</span>
            <span>${esc(item.source_event_id || "")}</span>
          </div>
        </div>
      `;
    }

    function inferFullFrameUrl(normalizedUrl) {
      if (!normalizedUrl) {
        return "";
      }
      const inferred = normalizedUrl.replace(/_norm\.[^./?#]+(?=([?#].*)?$)/, "_full.jpg");
      return inferred === normalizedUrl ? normalizedUrl : inferred;
    }

    function latestImage(mediaLinks, gazeEnabled) {
      const rows = normalizedImageItems(mediaLinks);
      if (!rows.length) {
        return `<div class="meta">no media links</div>`;
      }
      return imageRow(rows[rows.length - 1], gazeEnabled);
    }

    function imageHistory(identityKey, mediaLinks, gazeEnabled) {
      const rows = normalizedImageItems(mediaLinks);
      if (rows.length <= 1) {
        return "";
      }
      const detailsKey = `${identityKey}:images`;
      return `
        <details data-details-key="${esc(detailsKey)}" ${openDetails.has(detailsKey) ? "open" : ""}>
          <summary>Image history (${rows.length - 1})</summary>
          <div class="images">${rows.slice(0, -1).map(item => imageRow(item, gazeEnabled)).join("")}</div>
        </details>
      `;
    }

    function cameraStates(cameras) {
      if (!cameras || !cameras.length) {
        return `<div class="meta">no camera presence yet</div>`;
      }
      return `
        <div class="camera-states">
          ${cameras.map(camera => `
            <div class="camera-row">
              <div class="camera-id">${camera.state_type ? esc(camera.state_type) + " | " : ""}${esc(camera.camera_id || "")}</div>
              <div class="camera-metrics">
                presence ${esc(camera.presence_total_display || "0.0s")} |
                duration ${esc(camera.duration_display || "0.0s")} |
                not seen ${esc(camera.not_seen_display || "0.0s")} |
                TTL ${esc(camera.ttl_display || "")}
              </div>
            </div>
          `).join("")}
        </div>
      `;
    }

    function personMeta(identity) {
      if (identity.detection_type !== "person") {
        return "";
      }
      const faceLinks = identityLinks(identity.linked_face_identity_ids || [identity.linked_face_identity_id]);
      const bodyLinks = identityLinks(identity.linked_silhouette_identity_ids || [identity.linked_silhouette_identity_id]);
      return `
        <div class="meta">face ${faceLinks || ""}</div>
        <div class="meta">body ${bodyLinks || ""}</div>
        <div class="meta">confidence ${esc(identity.match_confidence || "")} | linked ${esc(identity.linked_at || "")}</div>
      `;
    }

    function interests(items) {
      if (!items || !items.length) return "";
      return `
        <div class="camera-states">
          ${items.map(item => `
            <div class="camera-row">
              <div class="camera-id">${item.intersects ? "👁 " : ""}shelf ${esc(item.shelf_name || item.shelf_id)} | camera ${esc(item.camera_id)}</div>
              <div class="camera-metrics">
                interest ${esc(item.total_display)}${item.category_id ? " | category " + esc(item.category_id) : ""}
              </div>
            </div>
          `).join("")}
        </div>
      `;
    }

    function identityLinks(identityIds) {
      return (identityIds || [])
        .filter(Boolean)
        .map(identityId => `
          <button class="jump-link" type="button" data-jump-identity="${esc(identityId)}">${esc(identityId)}</button>
        `)
        .join(", ");
    }

    function card(identity) {
      const fieldsKey = `${identity.key}:fields`;
      const isPerson = identity.detection_type === "person";
      const gazeEnabled = identity.detection_type === "silhouette";
      return `
        <article class="card" data-identity-id="${esc(identity.identity_id)}">
          <div class="card-head">
            <div class="identity">${esc(identity.identity_id)}</div>
            <div class="meta">${esc(identity.key)}</div>
            <div class="meta">TTL ${esc(identity.ttl_display || "")} | duration ${esc(identity.duration_display || "0.0s")} | not seen ${esc(identity.not_seen_display || "0.0s")}</div>
            <div class="meta">${esc(identity.detection_type)} ${identity.person_id ? "-> " + esc(identity.person_id) : ""}</div>
            ${personMeta(identity)}
            ${interests(identity.interests || [])}
            ${cameraStates(identity.camera_states || [])}
          </div>
          <details data-details-key="${esc(fieldsKey)}" ${openDetails.has(fieldsKey) ? "open" : ""}>
            <summary>Fields</summary>
            <div class="fields">${fieldRows(identity.fields)}</div>
          </details>
          ${isPerson ? "" : `<div class="images">${latestImage(identity.media_links || [], gazeEnabled)}</div>`}
          ${isPerson ? "" : imageHistory(identity.key, identity.media_links || [], gazeEnabled)}
        </article>
      `;
    }

    function rememberOpenDetails() {
      document.querySelectorAll("details[data-details-key]").forEach(details => {
        const key = details.getAttribute("data-details-key");
        if (!key) return;
        if (details.open) {
          openDetails.add(key);
        } else {
          openDetails.delete(key);
        }
      });
    }

    function render(data) {
      rememberOpenDetails();
      pollMs = data.poll_interval_ms || pollMs;
      document.getElementById("updated").textContent = `updated ${new Date(data.generated_at).toLocaleTimeString()}`;
      document.getElementById("total").textContent = `${data.identities.length} identities`;

      for (const bucket of Object.keys(lists)) {
        const items = data.identities.filter(item => item.bucket === bucket);
        counts[bucket].textContent = items.length;
        lists[bucket].innerHTML = items.length ? items.map(card).join("") : `<div class="empty">No active identities</div>`;
      }
    }

    function scrollToIdentity(identityId) {
      const cards = Array.from(document.querySelectorAll(".card[data-identity-id]"));
      const target = cards.find(card => card.dataset.identityId === identityId);
      if (!target) {
        document.getElementById("updated").textContent = `not found: ${identityId}`;
        return;
      }

      target.scrollIntoView({ behavior: "smooth", block: "center", inline: "nearest" });
      target.classList.add("highlight");
      setTimeout(() => target.classList.remove("highlight"), 1800);
    }

    function updateRefreshToggle() {
      refreshToggle.textContent = updatesPaused ? "▶" : "⏸";
      refreshToggle.title = updatesPaused ? "Resume updates" : "Pause updates";
      refreshToggle.setAttribute("aria-label", updatesPaused ? "Resume updates" : "Pause updates");
    }

    function schedulePoll() {
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      if (!updatesPaused) {
        pollTimer = setTimeout(poll, pollMs);
      }
    }

    async function poll() {
      if (updatesPaused) {
        return;
      }
      try {
        const response = await fetch("/api/state", { cache: "no-store" });
        render(await response.json());
      } catch (error) {
        document.getElementById("updated").textContent = `error: ${error}`;
      } finally {
        schedulePoll();
      }
    }

    refreshToggle.addEventListener("click", () => {
      updatesPaused = !updatesPaused;
      updateRefreshToggle();
      if (updatesPaused) {
        if (pollTimer) {
          clearTimeout(pollTimer);
          pollTimer = null;
        }
        document.getElementById("updated").textContent += " (paused)";
      } else {
        poll();
      }
    });

    updateRefreshToggle();
    poll();

    document.addEventListener("click", event => {
      const imageLink = event.target.closest("[data-gaze-overlay]");
      if (imageLink) {
        event.preventDefault();
        openGazeModal(
          imageLink.getAttribute("data-source-event-id") || "",
          imageLink.getAttribute("data-full-frame-url") || imageLink.getAttribute("href") || "",
        );
        return;
      }
      const button = event.target.closest("[data-jump-identity]");
      if (!button) {
        return;
      }
      scrollToIdentity(button.getAttribute("data-jump-identity"));
    });

    document.getElementById("gaze-modal-close").addEventListener("click", closeGazeModal);
    gazeModalBody.addEventListener("click", event => {
      if (event.target === gazeModalBody) closeGazeModal();
    });
    document.addEventListener("keydown", event => {
      if (event.key === "Escape" && gazeModal.classList.contains("open")) closeGazeModal();
    });

    document.getElementById("clear-hot-state").addEventListener("click", async event => {
      const button = event.currentTarget;
      if (!confirm("Clear all data in Redis Hot State?")) return;
      if (!confirm("This will delete every key from redis-hot-state. Continue?")) return;

      button.disabled = true;
      const previous = button.textContent;
      button.textContent = "Clearing...";
      const deleteMedia = document.getElementById("delete-media").checked;
      try {
        const response = await fetch("/api/admin/clear-hot-state", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ delete_media: deleteMedia }),
        });
        const result = await response.json();
        if (!response.ok) {
          throw new Error(result.detail || response.statusText);
        }
        const media = result.media || {};
        button.textContent = deleteMedia
          ? `Cleared ${result.deleted_keys} keys, ${media.deleted || 0} files`
          : `Cleared ${result.deleted_keys} keys`;
        await fetch("/api/state", { cache: "no-store" }).then(resp => resp.json()).then(render);
        setTimeout(() => { button.textContent = previous; button.disabled = false; }, 1800);
      } catch (error) {
        button.textContent = "Clear failed";
        alert(`Failed to clear hot state: ${error}`);
        setTimeout(() => { button.textContent = previous; button.disabled = false; }, 1800);
      }
    });
  </script>
</body>
</html>
"""
