import pytest

from app.main import InvalidOffer, _layout_id_from_content, _target


def test_group_target_is_mapped_to_xibo_display_group() -> None:
    assert _target({"player_group_id": "17"}) == ("displaygroup", 17)


def test_player_target_is_mapped_to_xibo_display() -> None:
    assert _target({"player_id": 6}) == ("display", 6)


def test_target_requires_exactly_one_player_selector() -> None:
    with pytest.raises(InvalidOffer, match="exactly one"):
        _target({"player_id": 6, "player_group_id": 17})
    with pytest.raises(InvalidOffer, match="exactly one"):
        _target({})


def test_content_accepts_plain_or_django_selector_layout_id() -> None:
    assert _layout_id_from_content({"content": "23"}) == 23
    assert _layout_id_from_content({"content": "23&Seasonal video"}) == 23


def test_content_requires_xibo_layout_id() -> None:
    with pytest.raises(InvalidOffer, match="layout id"):
        _layout_id_from_content({"content": "welcome video"})
