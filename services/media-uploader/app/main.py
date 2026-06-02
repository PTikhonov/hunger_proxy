from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Any

import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings


logger = logging.getLogger(settings.app_name)


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)


class MediaUploader:
    def __init__(self, redis: Redis, binary_redis: Redis, client: httpx.AsyncClient) -> None:
        self._redis = redis
        self._binary_redis = binary_redis
        self._client = client

    async def run(self) -> None:
        await self._ensure_group()
        logger.info(
            "Started %s stream=%s group=%s",
            settings.app_name,
            settings.media_save_jobs_stream,
            settings.consumer_group,
        )
        while True:
            for message_id, fields in await self._read():
                await self._handle(message_id, fields)

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                settings.media_save_jobs_stream,
                settings.consumer_group,
                id="$",
                mkstream=True,
            )
            logger.info("Created consumer group stream=%s group=%s", settings.media_save_jobs_stream, settings.consumer_group)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self) -> list[tuple[str, dict[str, str]]]:
        pending = await self._claim_pending()
        if pending:
            return pending

        response = await self._redis.xreadgroup(
            settings.consumer_group,
            settings.consumer_name,
            streams={settings.media_save_jobs_stream: ">"},
            count=settings.batch_size,
            block=settings.block_ms,
        )
        return [(str(message_id), fields) for _, messages in response for message_id, fields in messages]

    async def _claim_pending(self) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            settings.media_save_jobs_stream,
            settings.consumer_group,
            settings.consumer_name,
            min_idle_time=settings.pending_idle_ms,
            start_id="0-0",
            count=settings.batch_size,
        )
        claimed = response[1] if len(response) > 1 else []
        messages = [(str(message_id), fields) for message_id, fields in claimed]
        if messages:
            logger.info("Claimed pending media save jobs count=%s", len(messages))
        return messages

    async def _handle(self, message_id: str, fields: dict[str, str]) -> None:
        try:
            payload = json.loads(fields["payload"])
            event_id = str(payload["event_id"])
            media = payload.get("media") or {}

            uploaded = 0
            for field, info in media.items():
                if isinstance(info, dict) and await self._upload_media(event_id, field, info):
                    uploaded += 1

            await self._redis.xack(settings.media_save_jobs_stream, settings.consumer_group, message_id)
            logger.info("Processed media save job event_id=%s uploaded=%s message_id=%s", event_id, uploaded, message_id)
        except Exception:
            logger.exception("Failed to process media save job message_id=%s", message_id)

    async def _upload_media(self, event_id: str, field: str, info: dict[str, Any]) -> bool:
        redis_key = info.get("redis_key")
        upload_url = info.get("upload_url")
        if not redis_key or not upload_url:
            return False

        content = await self._binary_redis.get(redis_key)
        if not content:
            logger.info("Media bytes are missing or expired event_id=%s field=%s redis_key=%s", event_id, field, redis_key)
            return False

        response = await self._client.put(
            str(upload_url),
            content=content,
            headers={"Content-Type": str(info.get("content_type") or "application/octet-stream")},
        )
        if response.status_code not in {200, 201, 204}:
            raise RuntimeError(f"ffupload PUT failed status={response.status_code} body={response.text[:500]}")

        if settings.delete_after_upload:
            await self._binary_redis.delete(redis_key)

        logger.debug("Uploaded media event_id=%s field=%s bytes=%s url=%s", event_id, field, len(content), upload_url)
        return True


async def main() -> None:
    configure_logging()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    binary_redis = Redis.from_url(settings.redis_url, decode_responses=False)
    client = httpx.AsyncClient(timeout=settings.ffupload_timeout_seconds)
    try:
        await redis.ping()
        uploader = MediaUploader(redis, binary_redis, client)
        await uploader.run()
    finally:
        await client.aclose()
        await redis.aclose()
        await binary_redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
