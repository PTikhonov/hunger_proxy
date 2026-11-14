from __future__ import annotations

import json
import unittest

from app.main import MatcherService, PendingFace, _person_link_key


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.deleted: list[str] = []
        self.zrem_calls: list[tuple[str, str]] = []

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def exists(self, key: str) -> int:
        return int(key in self.hashes)

    async def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

    async def expire(self, key: str, seconds: int) -> None:
        del key, seconds

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.hashes.pop(key, None)

    async def zrem(self, key: str, member: str) -> None:
        self.zrem_calls.append((key, member))


class FakeStream:
    def __init__(self) -> None:
        self.events: list[dict[str, str]] = []

    async def append_person_event(self, fields: dict[str, str]) -> str:
        self.events.append(fields)
        return "1-0"


class PersonLinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirm_person_link_preserves_person_ownership(self) -> None:
        redis = FakeRedis()
        redis.hashes.update(
            {
                "person:p1": {"person_id": "person:p1"},
                "person:p2": {"person_id": "person:p2"},
                "identity:face:f1": {"person_id": "person:p1"},
                "identity:silhouette:s2": {"person_id": "person:p2"},
            }
        )
        stream = FakeStream()
        service = object.__new__(MatcherService)
        service._hot_state_redis = redis
        service._stream = stream

        pending = PendingFace(
            message_id="face-observation",
            observation={
                "identity_id": "face:f1",
                "camera_id": "camera:1",
                "source_event_id": "face-event",
            },
            face_embedding=[1.0],
            event_epoch=100.0,
            ready_at=0.0,
        )
        candidate = {
            "identity_id": "silhouette:s2",
            "person_id": "person:p2",
            "camera_id": "camera:1",
            "observation_id": "silhouette-observation",
            "source_event_id": "silhouette-event",
        }

        await service._confirm_person_link(
            face_person_id="person:p1",
            silhouette_person_id="person:p2",
            pending=pending,
            candidate=candidate,
            similarity=0.91,
            quality=0.98,
        )

        self.assertEqual(redis.hashes["identity:face:f1"]["person_id"], "person:p1")
        self.assertEqual(redis.hashes["identity:silhouette:s2"]["person_id"], "person:p2")
        self.assertEqual(
            json.loads(redis.hashes["person:p1"]["confirmed_related_person_ids"]), ["person:p2"]
        )
        self.assertEqual(
            json.loads(redis.hashes["person:p2"]["confirmed_related_person_ids"]), ["person:p1"]
        )
        relation = redis.hashes[_person_link_key("person:p1", "person:p2")]
        self.assertEqual(relation["status"], "confirmed")
        self.assertEqual(relation["similarity"], "0.91")
        self.assertEqual(stream.events[0]["event_type"], "person_link_confirmed")

    async def test_linked_face_uses_pending_silhouette_without_reassigning_identity(self) -> None:
        redis = FakeRedis()
        redis.hashes.update(
            {
                "person:p1": {"person_id": "person:p1"},
                "person:p2": {"person_id": "person:p2"},
                "identity:face:f1": {"person_id": "person:p1"},
                "identity:silhouette:s2": {"person_id": "person:p2"},
            }
        )
        stream = FakeStream()
        service = object.__new__(MatcherService)
        service._hot_state_redis = redis
        service._stream = stream
        candidate = {
            "identity_id": "silhouette:s2",
            "person_id": "person:p2",
            "camera_id": "camera:1",
            "observation_id": "silhouette-observation",
            "source_event_id": "silhouette-event",
            "time_distance": "0",
        }

        async def pending_candidates(camera_id: str, event_epoch: float) -> list[dict[str, str]]:
            self.assertEqual((camera_id, event_epoch), ("camera:1", 100.0))
            return [candidate]

        async def candidate_face(pending: PendingFace, value: dict[str, str]):
            self.assertEqual(pending.message_id, "face-observation")
            self.assertIs(value, candidate)
            return type("Face", (), {"embedding": [1.0], "quality": 0.99})()

        service._pending_silhouette_candidates = pending_candidates
        service._face_embedding_from_silhouette = candidate_face
        pending = PendingFace(
            message_id="face-observation",
            observation={
                "identity_id": "face:f1",
                "camera_id": "camera:1",
                "source_event_id": "face-event",
            },
            face_embedding=[1.0],
            event_epoch=100.0,
            ready_at=0.0,
        )

        await service._match_face(pending)

        self.assertEqual(redis.hashes["identity:face:f1"]["person_id"], "person:p1")
        self.assertEqual(redis.hashes["identity:silhouette:s2"]["person_id"], "person:p2")
        self.assertEqual(stream.events[0]["event_type"], "person_link_confirmed")
