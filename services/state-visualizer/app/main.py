from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI
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


@app.post("/api/admin/clear-hot-state")
async def clear_hot_state() -> dict[str, Any]:
    assert redis is not None
    deleted = 0
    async for key in redis.scan_iter(match="*", count=settings.identity_scan_count):
        deleted += await redis.delete(key)
    return {
        "status": "ok",
        "deleted_keys": deleted,
        "cleared_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


def _identity_from_hash(key: str, fields: dict[str, str], ttl_seconds: int) -> dict[str, Any]:
    media_links = _json_list(fields.get("media_links"))
    media_links.sort(key=lambda item: str(item.get("event_timestamp") or item.get("source_event_id") or ""))
    display_fields = _display_fields(fields)

    detection_type = fields.get("detection_type") or ""
    person_id = fields.get("person_id") or fields.get("matched_person_id") or fields.get("person_identity_id") or ""
    bucket = "matched" if person_id else ("silhouette" if detection_type == "silhouette" else "face")

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


def _float_or_zero(value: str | None) -> float:
    try:
        return float(value or 0.0)
    except ValueError:
        return 0.0


def _display_fields(fields: dict[str, str]) -> dict[str, str]:
    hidden = {"media_links"}
    return {
        key: _display_value(key, value)
        for key, value in fields.items()
        if key not in hidden
    }


def _display_value(key: str, value: str) -> str:
    if key in {"first_seen_epoch", "last_seen_epoch"}:
        return _format_epoch(value)
    if key in {"last_seen_at", "first_seen_at", "event_timestamp", "updated_at"}:
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

    function imageRow(item) {
      const normalizedUrl = item.normalized.public_url || item.normalized.upload_url || "";
      const fullFrameUrl = item.fullFrame
        ? (item.fullFrame.public_url || item.fullFrame.upload_url || normalizedUrl)
        : inferFullFrameUrl(normalizedUrl);
      return `
        <div class="image-row">
          <a href="${esc(fullFrameUrl)}" target="_blank" rel="noreferrer">
            <img class="thumb" src="${esc(normalizedUrl)}" loading="lazy" alt="normalized" />
          </a>
          <div class="link-meta">
            <strong>normalized</strong>
            <a href="${esc(fullFrameUrl)}" target="_blank" rel="noreferrer">open full_frame</a>
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

    function latestImage(mediaLinks) {
      const rows = normalizedImageItems(mediaLinks);
      if (!rows.length) {
        return `<div class="meta">no media links</div>`;
      }
      return imageRow(rows[rows.length - 1]);
    }

    function imageHistory(identityKey, mediaLinks) {
      const rows = normalizedImageItems(mediaLinks);
      if (rows.length <= 1) {
        return "";
      }
      const detailsKey = `${identityKey}:images`;
      return `
        <details data-details-key="${esc(detailsKey)}" ${openDetails.has(detailsKey) ? "open" : ""}>
          <summary>Image history (${rows.length - 1})</summary>
          <div class="images">${rows.slice(0, -1).map(imageRow).join("")}</div>
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
              <div class="camera-id">${esc(camera.camera_id || "")}</div>
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

    function card(identity) {
      const fieldsKey = `${identity.key}:fields`;
      return `
        <article class="card">
          <div class="card-head">
            <div class="identity">${esc(identity.identity_id)}</div>
            <div class="meta">${esc(identity.key)}</div>
            <div class="meta">TTL ${esc(identity.ttl_display || "")} | duration ${esc(identity.duration_display || "0.0s")} | not seen ${esc(identity.not_seen_display || "0.0s")}</div>
            <div class="meta">${esc(identity.detection_type)} ${identity.person_id ? "-> " + esc(identity.person_id) : ""}</div>
            ${cameraStates(identity.camera_states || [])}
          </div>
          <details data-details-key="${esc(fieldsKey)}" ${openDetails.has(fieldsKey) ? "open" : ""}>
            <summary>Fields</summary>
            <div class="fields">${fieldRows(identity.fields)}</div>
          </details>
          <div class="images">${latestImage(identity.media_links || [])}</div>
          ${imageHistory(identity.key, identity.media_links || [])}
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

    async function poll() {
      try {
        const response = await fetch("/api/state", { cache: "no-store" });
        render(await response.json());
      } catch (error) {
        document.getElementById("updated").textContent = `error: ${error}`;
      } finally {
        setTimeout(poll, pollMs);
      }
    }

    poll();

    document.getElementById("clear-hot-state").addEventListener("click", async event => {
      const button = event.currentTarget;
      if (!confirm("Clear all data in Redis Hot State?")) return;
      if (!confirm("This will delete every key from redis-hot-state. Continue?")) return;

      button.disabled = true;
      const previous = button.textContent;
      button.textContent = "Clearing...";
      try {
        const response = await fetch("/api/admin/clear-hot-state", { method: "POST" });
        const result = await response.json();
        if (!response.ok) {
          throw new Error(result.detail || response.statusText);
        }
        button.textContent = `Cleared ${result.deleted_keys} keys`;
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
