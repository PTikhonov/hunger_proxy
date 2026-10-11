from types import SimpleNamespace

import pytest

from app.extraction import ExtractionClient, HeadNotDetectedError


def _client() -> ExtractionClient:
    client = object.__new__(ExtractionClient)
    client._settings = SimpleNamespace(
        head_pose_attribute="head_headpose_unconstrained",
        min_head_size=30,
        max_head_width_to_height_rate=1.0,
        max_head_height_to_body_height=0.5,
        max_head_to_body_proportion=0.5,
    )
    return client


def _head(width: float, height: float, score: float = 1.0) -> dict:
    return {
        "bbox": {"left": 0, "top": 0, "right": width, "bottom": height},
        "detection_score": score,
        "attributes": {
            "head_headpose_unconstrained": {
                "result": {"pitch": 1, "roll": 2, "yaw": 3},
            }
        },
    }


def _payload(*heads: dict) -> dict:
    return {"responses": [{"objects": {"head": list(heads)}}]}


def test_parse_accepts_head_at_configured_limits() -> None:
    bbox, pose = _client()._parse(_payload(_head(30, 30)), body_width=100, body_height=100)
    assert bbox == [0.0, 0.0, 30.0, 30.0]
    assert pose == {"pitch": 1.0, "roll": 2.0, "yaw": 3.0}


@pytest.mark.parametrize(
    "head",
    [
        _head(29, 40),
        _head(40, 29),
        _head(40, 30),
        _head(30, 51),
    ],
)
def test_parse_rejects_head_that_violates_geometry_constraints(head: dict) -> None:
    with pytest.raises(HeadNotDetectedError):
        _client()._parse(_payload(head), body_width=100, body_height=100)


def test_parse_selects_highest_score_among_eligible_heads() -> None:
    bbox, _ = _client()._parse(
        _payload(_head(20, 80, score=1.0), _head(30, 40, score=0.8)),
        body_width=100,
        body_height=200,
    )
    assert bbox == [0.0, 0.0, 30.0, 40.0]
