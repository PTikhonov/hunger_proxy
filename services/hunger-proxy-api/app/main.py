from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Annotated, AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from redis.asyncio import Redis

from app.config import settings
from app.schemas import (
    ApplyResult,
    CameraGeometrySnapshot,
    CameraGeometryTombstone,
    GeometryListItem,
    GeometryRecord,
)
from app.storage import GeometryStore, RevisionConflictError


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis = Redis.from_url(settings.redis_geometry_url, decode_responses=True)
    await redis.ping()
    app.state.redis = redis
    app.state.geometry_store = GeometryStore(redis, settings)
    try:
        yield
    finally:
        await redis.aclose()


app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)


def require_service_token(authorization: Annotated[str | None, Header()] = None) -> None:
    if not settings.service_token:
        return
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(token, settings.service_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid service token")


def geometry_store(request: Request) -> GeometryStore:
    return request.app.state.geometry_store


StoreDependency = Annotated[GeometryStore, Depends(geometry_store)]


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    await request.app.state.redis.ping()
    return {"status": "ok", "service": settings.app_name}


@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    await request.app.state.redis.ping()
    return {"status": "ready", "service": settings.app_name}


@app.put(
    "/internal/v1/cameras/{camera_id}/geometry",
    response_model=ApplyResult,
    dependencies=[Depends(require_service_token)],
)
async def put_geometry(camera_id: str, snapshot: CameraGeometrySnapshot, store: StoreDependency) -> ApplyResult:
    if str(snapshot.camera_id) != camera_id:
        raise HTTPException(status_code=422, detail="camera_id in path and body must match")
    return await _apply(store, snapshot.model_dump(mode="json"))


@app.delete(
    "/internal/v1/cameras/{camera_id}/geometry",
    response_model=ApplyResult,
    dependencies=[Depends(require_service_token)],
)
async def delete_geometry(
    camera_id: str,
    revision: Annotated[int, Query(ge=1)],
    store: StoreDependency,
) -> ApplyResult:
    tombstone = CameraGeometryTombstone(camera_id=camera_id, revision=revision)
    return await _apply(store, tombstone.model_dump(mode="json"))


@app.get(
    "/internal/v1/cameras/{camera_id}/geometry",
    response_model=GeometryRecord,
    dependencies=[Depends(require_service_token)],
)
async def get_geometry(camera_id: str, store: StoreDependency) -> GeometryRecord:
    record = await store.get(camera_id)
    if record is None:
        raise HTTPException(status_code=404, detail="camera geometry not found")
    return GeometryRecord.model_validate(record)


@app.get(
    "/internal/v1/cameras",
    response_model=list[GeometryListItem],
    dependencies=[Depends(require_service_token)],
)
async def list_cameras(
    store: StoreDependency,
    include_deleted: bool = False,
) -> list[GeometryListItem]:
    return [GeometryListItem.model_validate(item) for item in await store.list(include_deleted=include_deleted)]


async def _apply(store: GeometryStore, payload: dict[str, object]) -> ApplyResult:
    try:
        result = await store.apply(payload)
    except RevisionConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "reason": exc.reason,
                "camera_id": exc.camera_id,
                "incoming_revision": exc.incoming_revision,
                "current_revision": exc.current_revision,
            },
        ) from exc
    return ApplyResult.model_validate(result.__dict__)
