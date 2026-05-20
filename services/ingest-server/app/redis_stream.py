from __future__ import annotations

from redis.asyncio import Redis

from app.config import Settings
from app.schemas import DetectionEvent


class DetectionStreamWriter:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def append(self, event: DetectionEvent) -> str:
        stream_id = await self._redis.xadd(
            self._settings.detections_stream,
            event.stream_fields(),
            maxlen=self._settings.stream_maxlen,
            approximate=True,
        )
        if isinstance(stream_id, bytes):
            return stream_id.decode("utf-8")
        return str(stream_id)

