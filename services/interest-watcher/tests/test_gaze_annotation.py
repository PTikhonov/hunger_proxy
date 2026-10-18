import asyncio
import json

from app.config import settings
from app.main import InterestWatcher


class FakeHotState:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int | None]] = []

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.calls.append((key, value, ex))


def test_gaze_annotation_is_stored_per_source_event() -> None:
    hot = FakeHotState()
    watcher = object.__new__(InterestWatcher)
    watcher.hot = hot
    asyncio.run(watcher._store_gaze_annotation(
        observation={
            "source_event_id": "event-1",
            "event_timestamp": "2026-10-18T17:18:33.641Z",
            "event_epoch": 1792343913.641,
        },
        message_id="1792343913662-0",
        identity_id="silhouette:test",
        person_id="person:test",
        camera_id="hngr-camera:44",
        geometry={
            "revision": 183,
            "camera": {
                "frame_width": 1280,
                "frame_height": 720,
                "focal_length_in_mm": 2.8,
                "sensor_width_in_mm": 4.92,
                "sensor_height_in_mm": 2.77,
            },
        },
        head_bbox=[701.0, 0.0, 804.0, 119.0],
        head_pose={"pitch": 15.375, "roll": -5.953125, "yaw": -16.125},
        head_point_3d=(0.1544, -0.4129, 1.529),
        gaze_direction=(0.2677940801, -0.2651354262, 0.9262772460),
        shelves=[{
            "shelf_id": "39",
            "name": "стол",
            "points_px": [[950.0, 330.0], [966.0, 490.0], [1107.0, 426.0], [1086.0, 293.2]],
            "intersects": False,
            "intersection_px": [824.1, 154.0],
        }],
    ))

    assert len(hot.calls) == 1
    key, raw, ttl = hot.calls[0]
    payload = json.loads(raw)
    assert key == "gaze_annotation:event-1"
    assert ttl == settings.person_ttl_seconds
    assert payload["geometry_revision"] == 183
    assert payload["head_distance_m"] == 1.529
    assert payload["shelves"][0]["points_px"][0] == [950.0, 330.0]
    assert payload["gaze_direction_px"][0] > 0
    assert payload["gaze_direction_px"][1] < 0
