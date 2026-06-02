from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import Settings
from app.schemas import IdentityObservation


logger = logging.getLogger(__name__)


class StreamClient:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._settings.detections_stream,
                self._settings.detections_consumer_group,
                id="$",
                mkstream=True,
            )
            logger.info(
                "Created consumer group stream=%s group=%s",
                self._settings.detections_stream,
                self._settings.detections_consumer_group,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(self) -> list[tuple[str, dict[str, str]]]:
        pending = await self._claim_pending()
        if pending:
            return pending

        response = await self._redis.xreadgroup(
            self._settings.detections_consumer_group,
            self._settings.detections_consumer_name,
            streams={self._settings.detections_stream: ">"},
            count=self._settings.batch_size,
            block=self._settings.block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _, stream_messages in response:
            for message_id, fields in stream_messages:
                messages.append((str(message_id), fields))
        return messages

    async def _claim_pending(self) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            self._settings.detections_stream,
            self._settings.detections_consumer_group,
            self._settings.detections_consumer_name,
            min_idle_time=self._settings.pending_idle_ms,
            start_id="0-0",
            count=self._settings.batch_size,
        )

        claimed = response[1] if len(response) > 1 else []
        messages = [(str(message_id), fields) for message_id, fields in claimed]
        if messages:
            logger.info(
                "Claimed pending detection messages count=%s stream=%s group=%s",
                len(messages),
                self._settings.detections_stream,
                self._settings.detections_consumer_group,
            )
        return messages

    async def ack(self, message_id: str) -> None:
        await self._redis.xack(
            self._settings.detections_stream,
            self._settings.detections_consumer_group,
            message_id,
        )

    async def append_identity_observation(self, observation: IdentityObservation) -> str:
        stream_id = await self._redis.xadd(
            self._settings.identity_observations_stream,
            observation.stream_fields(),
            maxlen=self._settings.stream_maxlen,
            approximate=True,
        )
        return str(stream_id)
