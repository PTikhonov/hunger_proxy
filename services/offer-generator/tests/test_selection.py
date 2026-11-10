from app.main import _action_is_active, _age_matches, _delivery_type, _offer_action, _select_promotion


def test_selects_highest_priority_matching_context_and_keeps_all_its_actions() -> None:
    payload = {
        "rules": [
            {
                "context_id": 10, "priority": 1, "category_id": 5, "interest_lvl": 2,
                "gender": "ALL", "age_range": {"from_age": 0, "to_age": 100},
                "action": {"id": 1, "type": "Telegram"},
            },
            {
                "context_id": 20, "priority": 9, "category_id": 5, "interest_lvl": 2,
                "gender": "male", "age_range": {"from_age": 17, "to_age": 35},
                "action": {"id": 2, "type": "Видео-обращение"},
            },
            {
                "context_id": 20, "priority": 9, "category_id": 5, "interest_lvl": 2,
                "gender": "male", "age_range": {"from_age": 17, "to_age": 35},
                "action": {"id": 3, "type": "Telegram"},
            },
        ]
    }

    selected = _select_promotion(payload, {"face_age": "30", "face_gender": "male"}, 5, 2)

    assert selected is not None
    assert selected["context_id"] == 20
    assert [action["id"] for action in selected["actions"]] == [2, 3]


def test_face_age_has_priority_over_silhouette_bucket() -> None:
    person = {"face_age": "40", "silhouette_age": "17-35"}
    assert not _age_matches(person, 17, 35)
    assert _age_matches(person, 36, 50)


def test_single_matching_context_does_not_depend_on_priority() -> None:
    payload = {
        "rules": [{
            "context_id": 10, "priority": -100, "category_id": 5, "interest_lvl": 2,
            "gender": "ALL", "age_range": {"from_age": 0, "to_age": 100},
            "action": {"id": 1, "type": "Telegram"},
        }]
    }

    selected = _select_promotion(payload, {"face_age": "30", "face_gender": "male"}, 5, 2)

    assert selected is not None
    assert selected["context_id"] == 10


def test_delivery_type_mapping() -> None:
    assert _delivery_type({"type": "Аудио-сообщение"}) == "xibo_audio"
    assert _delivery_type({"type": "Скрипт на кассе"}) == "cashier_script"


def test_action_contains_separate_personal_and_channel_cooldowns() -> None:
    action = _offer_action(
        {"id": 4, "type": "Telegram"},
        {
            "cooldowns_seconds": {"telegram": 30},
            "channel_cooldowns_seconds": {"telegram": 1},
        },
    )

    assert action["personal_cooldown_seconds"] == 30
    assert action["channel_cooldown_seconds"] == 1


def test_action_window_is_enforced() -> None:
    assert not _action_is_active({"active_to": "2020-01-01T00:00:00Z"})
    assert _action_is_active({"active_from": "2020-01-01T00:00:00Z"})
