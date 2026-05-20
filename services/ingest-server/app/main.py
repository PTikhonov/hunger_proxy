from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request, status
from redis.asyncio import Redis

from app.config import settings
from app.normalizer import normalize_request
from app.redis_stream import DetectionStreamWriter


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("uvicorn").setLevel(getattr(logging, settings.log_level))
    logging.getLogger("uvicorn.access").setLevel(getattr(logging, settings.log_level))


configure_logging()
logger = logging.getLogger(settings.app_name)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Starting %s with LOG_LEVEL=%s", settings.app_name, settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    await redis.ping()
    app.state.redis = redis
    app.state.stream_writer = DetectionStreamWriter(redis, settings)
    logger.info("Connected to Redis and ready to append to stream %s", settings.detections_stream)
    try:
        yield
    finally:
        logger.info("Shutting down %s", settings.app_name)
        await redis.aclose()


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)


async def log_full_request_debug(request: Request) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return

    body = await request.body()
    headers = {key: value for key, value in request.headers.items()}
    body_text = body.decode("utf-8", errors="replace")
    logger.debug(
        "Full incoming request: method=%s url=%s client=%s headers=%s body=%s",
        request.method,
        str(request.url),
        request.client.host if request.client else None,
        headers,
        body_text,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    await app.state.redis.ping()
    return {"status": "ok", "service": settings.app_name}


async def ingest(request: Request) -> dict[str, str]:
    try:
        await log_full_request_debug(request)
        event = await normalize_request(request)
        stream_id = await request.app.state.stream_writer.append(event)
    except ValueError as exc:
        logger.info("Rejected request: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("Failed to append detection event")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to append detection event: {exc}",
        ) from exc

    logger.info(
        "Accepted detection event event_id=%s stream=%s stream_id=%s camera_id=%s event_timestamp=%s",
        event.event_id,
        settings.detections_stream,
        stream_id,
        event.camera_id,
        event.event_timestamp,
    )

    return {
        "status": "accepted",
        "event_id": event.event_id,
        "stream": settings.detections_stream,
        "stream_id": stream_id,
    }


@app.post("/v1/detections", status_code=status.HTTP_202_ACCEPTED)
async def post_detection(request: Request) -> dict[str, str]:
    return await ingest(request)


@app.post("/context_pers", status_code=status.HTTP_202_ACCEPTED)
async def post_legacy_context_pers(request: Request) -> dict[str, str]:
    return await ingest(request)
