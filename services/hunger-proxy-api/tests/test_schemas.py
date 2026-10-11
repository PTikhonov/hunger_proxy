from app.schemas import ShelfGeometry


POINTS = [(0, 0, 1), (0, 1, 1), (1, 1, 1), (1, 0, 1)]


def test_legacy_side_is_viewed_opposite_to_ui_arrow() -> None:
    assert ShelfGeometry(shelf_id=1, side="A", points=POINTS).viewing_direction == "normal"
    assert ShelfGeometry(shelf_id=1, side="B", points=POINTS).viewing_direction == "opposite"
    assert ShelfGeometry(shelf_id=1, side="AB", points=POINTS).viewing_direction == "both"


def test_explicit_viewing_direction_is_preserved() -> None:
    shelf = ShelfGeometry(
        shelf_id=1,
        side="A",
        viewing_direction="opposite",
        points=POINTS,
    )
    assert shelf.viewing_direction == "opposite"

