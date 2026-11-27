from __future__ import annotations

import json

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.config import Settings


class RevisionConflictError(Exception):
    def __init__(self, project_id: int, incoming_revision: int, current_revision: int) -> None:
        self.project_id = project_id
        self.incoming_revision = incoming_revision
        self.current_revision = current_revision


class DeviceRegistry:
    """Stores complete project snapshots, accepting only monotonic revisions."""

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    def _key(self, project_id: int) -> str:
        return f"{self.settings.device_key_prefix}:{project_id}"

    async def apply(self, payload: dict) -> str:
        project_id = int(payload["project_id"])
        incoming_revision = int(payload["revision"])
        key = self._key(project_id)
        while True:
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = await pipe.hget(key, "revision")
                    current_revision = int(current or 0)
                    if incoming_revision < current_revision:
                        raise RevisionConflictError(project_id, incoming_revision, current_revision)
                    if incoming_revision == current_revision:
                        return "unchanged"
                    pipe.multi()
                    pipe.hset(key, mapping={
                        "revision": incoming_revision,
                        "payload": json.dumps(payload, separators=(",", ":")),
                    })
                    await pipe.execute()
                    return "applied"
                except WatchError:
                    continue

    async def get(self, project_id: int) -> dict | None:
        payload = await self.redis.hget(self._key(project_id), "payload")
        return json.loads(payload) if payload else None
