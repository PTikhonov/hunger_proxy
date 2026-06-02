from __future__ import annotations

import math
import time
from dataclasses import dataclass
from uuid import uuid4


@dataclass
class IdentityRecord:
    identity_id: str
    detection_type: str
    embedding: list[float]
    first_seen_ts: float
    last_seen_ts: float


@dataclass(frozen=True)
class IdentityResolution:
    identity: IdentityRecord
    is_new: bool
    confidence: float | None


class IdentityRegistry:
    def __init__(self, thresholds: dict[str, float], ttl_seconds: int) -> None:
        self._thresholds = thresholds
        self._ttl_seconds = ttl_seconds
        self._records: dict[str, list[IdentityRecord]] = {}

    def resolve(self, detection_type: str, embedding: list[float]) -> IdentityResolution:
        now = time.time()
        self._drop_expired(now)

        best_record: IdentityRecord | None = None
        best_confidence: float | None = None

        for record in self._records.get(detection_type, []):
            confidence = cosine_similarity(record.embedding, embedding)
            if best_confidence is None or confidence > best_confidence:
                best_confidence = confidence
                best_record = record

        threshold = self._thresholds.get(detection_type, 1.0)
        if best_record is not None and best_confidence is not None and best_confidence >= threshold:
            best_record.embedding = embedding
            best_record.last_seen_ts = now
            return IdentityResolution(identity=best_record, is_new=False, confidence=best_confidence)

        identity = IdentityRecord(
            identity_id=f"{detection_type}:{uuid4()}",
            detection_type=detection_type,
            embedding=embedding,
            first_seen_ts=now,
            last_seen_ts=now,
        )
        self._records.setdefault(detection_type, []).append(identity)
        return IdentityResolution(identity=identity, is_new=True, confidence=best_confidence)

    def _drop_expired(self, now: float) -> None:
        cutoff = now - self._ttl_seconds
        for detection_type, records in list(self._records.items()):
            active = [record for record in records if record.last_seen_ts >= cutoff]
            if active:
                self._records[detection_type] = active
            else:
                self._records.pop(detection_type, None)


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0

    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0

    raw = dot / (left_norm * right_norm)
    return max(0.0, min(1.0, (raw + 1.0) / 2.0))
