from app.main import _matched_category_id


POINTS = [(0.0, 0.0, 1.0), (1.0, 0.0, 1.0), (0.0, 1.0, 1.0)]


def test_selects_category_for_the_approached_shelf_side() -> None:
    shelf = {
        "points": POINTS,
        "category_id": 10,
        "side": "A",
        "category_2_id": 11,
        "side_2": "B",
    }

    assert _matched_category_id(shelf, (0.0, 0.0, 1.0)) == 10
    assert _matched_category_id(shelf, (0.0, 0.0, -1.0)) == 11


def test_returns_no_category_when_the_approached_side_has_no_category() -> None:
    shelf = {"points": POINTS, "category_id": 10, "side": "A"}

    assert _matched_category_id(shelf, (0.0, 0.0, -1.0)) is None
