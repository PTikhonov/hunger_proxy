from app.main import _viewing_direction


def test_explicit_viewing_direction_is_preserved() -> None:
    assert _viewing_direction({"viewing_direction": "normal"}) == "normal"
    assert _viewing_direction({"viewing_direction": "opposite"}) == "opposite"
    assert _viewing_direction({"viewing_direction": "both"}) == "both"


def test_legacy_side_is_viewed_opposite_to_ui_arrow() -> None:
    assert _viewing_direction({"side": "A"}) == "normal"
    assert _viewing_direction({"side": "B"}) == "opposite"
    assert _viewing_direction({"side": "AB"}) == "both"

