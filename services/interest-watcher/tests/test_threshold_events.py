from app.main import _threshold_rules


def test_builds_category_threshold_rule_with_person_scoped_cooldown_key() -> None:
    payload = {
        "revision": 8,
        "thresholds_seconds": {"passed": 1, "attention": 5, "involved": 15, "buy": 45},
        "cooldowns_seconds": {"audio": 120, "video": 60, "telegram": 10},
        "rules": [
            {
                "context_id": 12,
                "priority": 9,
                "category_id": 5,
                "interest_lvl": 2,
                "action": {"id": 31, "type": "Видео-обращение"},
            },
            {"context_id": 13, "category_id": 6, "interest_lvl": 2, "action": {"id": 32}},
        ],
    }

    rules = _threshold_rules(payload, "person:test", 1, 5)

    assert rules == [{
        "context_id": 12,
        "rule_id": 31,
        "interest_lvl": 2,
        "priority": 9,
        "threshold": 5,
        "cooldown_seconds": 60,
        "cooldown_key": "interest-threshold:cooldown:person:test:1:5:12:31:5",
        "rule_revision": 8,
        "action": {"id": 31, "type": "Видео-обращение"},
    }]


def test_global_category_rule_is_eligible_for_each_category() -> None:
    payload = {
        "revision": 1,
        "thresholds_seconds": {"passed": 1},
        "cooldowns_seconds": {"video": 3},
        "rules": [{"context_id": 4, "category_id": None, "interest_lvl": 1, "action": {"id": 5}}],
    }

    assert len(_threshold_rules(payload, "person:test", 2, 99)) == 1
