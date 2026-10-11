from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal

from redis.asyncio import Redis

from app.config import Settings


APPLY_GEOMETRY_SCRIPT = """
local key = KEYS[1]
local index_key = KEYS[2]
local incoming_revision = tonumber(ARGV[1])
local incoming_checksum = ARGV[2]
local schema_version = ARGV[3]
local updated_at = ARGV[4]
local payload = ARGV[5]
local camera_id = ARGV[6]
local deleted = ARGV[7]
local channel = ARGV[8]
local event = ARGV[9]

local current_revision_raw = redis.call('HGET', key, 'revision')
if current_revision_raw then
  local current_revision = tonumber(current_revision_raw)
  if incoming_revision < current_revision then
    return {'stale', tostring(current_revision), redis.call('HGET', key, 'checksum') or ''}
  end
  if incoming_revision == current_revision then
    local current_checksum = redis.call('HGET', key, 'checksum') or ''
    if current_checksum == incoming_checksum then
      return {'unchanged', tostring(current_revision), current_checksum}
    end
    return {'conflict', tostring(current_revision), current_checksum}
  end
end

local existed = redis.call('EXISTS', key)
redis.call('HSET', key,
  'camera_id', camera_id,
  'revision', tostring(incoming_revision),
  'schema_version', schema_version,
  'checksum', incoming_checksum,
  'updated_at', updated_at,
  'deleted', deleted,
  'payload', payload)
redis.call('SADD', index_key, camera_id)
redis.call('PUBLISH', channel, event)

if deleted == 'true' then
  return {'deleted', tostring(incoming_revision), incoming_checksum}
end
if existed == 0 then
  return {'created', tostring(incoming_revision), incoming_checksum}
end
return {'updated', tostring(incoming_revision), incoming_checksum}
"""


class RevisionConflictError(RuntimeError):
    def __init__(self, camera_id: str, incoming_revision: int, current_revision: int, reason: str) -> None:
        super().__init__(reason)
        self.camera_id = camera_id
        self.incoming_revision = incoming_revision
        self.current_revision = current_revision
        self.reason = reason


@dataclass(frozen=True)
class StoredApplyResult:
    status: Literal["created", "updated", "unchanged", "deleted"]
    camera_id: str
    revision: int
    checksum: str


class GeometryStore:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def apply(self, payload: dict[str, Any]) -> StoredApplyResult:
        camera_id = str(payload["camera_id"])
        revision = int(payload["revision"])
        deleted = bool(payload.get("deleted", False))
        canonical_payload = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        checksum = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        event = json.dumps(
            {
                "event_type": "camera_geometry_deleted" if deleted else "camera_geometry_updated",
                "camera_id": camera_id,
                "revision": revision,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        result = await self._redis.eval(
            APPLY_GEOMETRY_SCRIPT,
            2,
            self._key(camera_id),
            self._settings.geometry_camera_index,
            revision,
            checksum,
            payload["schema_version"],
            payload["updated_at"],
            canonical_payload,
            camera_id,
            "true" if deleted else "false",
            self._settings.geometry_changed_channel,
            event,
        )
        status = str(result[0])
        current_revision = int(result[1])
        current_checksum = str(result[2])
        if status in {"stale", "conflict"}:
            reason = "stale_revision" if status == "stale" else "revision_checksum_conflict"
            raise RevisionConflictError(camera_id, revision, current_revision, reason)
        return StoredApplyResult(
            status=status,  # type: ignore[arg-type]
            camera_id=camera_id,
            revision=current_revision,
            checksum=current_checksum,
        )

    async def get(self, camera_id: str) -> dict[str, Any] | None:
        fields = await self._redis.hgetall(self._key(camera_id))
        if not fields:
            return None
        return {
            "camera_id": str(fields["camera_id"]),
            "revision": int(fields["revision"]),
            "schema_version": int(fields["schema_version"]),
            "checksum": str(fields["checksum"]),
            "updated_at": str(fields["updated_at"]),
            "deleted": str(fields.get("deleted", "false")).lower() == "true",
            "payload": json.loads(fields["payload"]),
        }

    async def list(self, include_deleted: bool = False) -> list[dict[str, Any]]:
        camera_ids = sorted(str(value) for value in await self._redis.smembers(self._settings.geometry_camera_index))
        result: list[dict[str, Any]] = []
        for camera_id in camera_ids:
            record = await self.get(camera_id)
            if record is None or (record["deleted"] and not include_deleted):
                continue
            result.append(
                {
                    "camera_id": record["camera_id"],
                    "revision": record["revision"],
                    "updated_at": record["updated_at"],
                    "deleted": record["deleted"],
                }
            )
        return result

    def _key(self, camera_id: str) -> str:
        return f"{self._settings.geometry_key_prefix}:{camera_id}"
