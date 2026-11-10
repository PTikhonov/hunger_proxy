from app.schemas import PromotionRulesSnapshot, ShelfGeometry


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


def test_promotion_rules_snapshot_accepts_complete_location_rules() -> None:
    snapshot = PromotionRulesSnapshot(
        location_id=1,
        revision=2,
        thresholds_seconds={"passed": 1, "attention": 5, "involved": 15, "buy": 45},
        cooldowns_seconds={"audio": 120, "video": 60, "telegram": 10},
        channel_cooldowns_seconds={"audio": 5, "video": 5, "telegram": 1},
        rules=[],
    )
    assert snapshot.location_id == 1
