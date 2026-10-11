from app.geometry import gaze_vector, ray_intersects_shelf


def test_forward_ray_and_normal_side_hit() -> None:
    shelf = [(0, 0, 1), (0, 1, 1), (1, 1, 1), (1, 0, 1)]
    hit, point = ray_intersects_shelf((0.5, 0.5, 0), (0, 0, 1), shelf, "opposite")
    assert hit
    assert point == (0.5, 0.5, 1.0)


def test_wrong_side_and_backward_ray_do_not_hit() -> None:
    shelf = [(0, 0, 1), (0, 1, 1), (1, 1, 1), (1, 0, 1)]
    assert not ray_intersects_shelf((0.5, 0.5, 0), (0, 0, 1), shelf, "normal")[0]
    assert not ray_intersects_shelf((0.5, 0.5, 0), (0, 0, -1), shelf, "both")[0]


def test_head_pose_uses_camera_coordinate_axes() -> None:
    vector = gaze_vector(yaw_degrees=49.875, pitch_degrees=-26.296875, roll_degrees=-12.65625)
    expected = (-0.6855081088, 0.4430222944, 0.5777628228)
    assert all(abs(actual - wanted) < 1e-9 for actual, wanted in zip(vector, expected, strict=True))


def test_frontal_head_pose_points_away_from_camera() -> None:
    assert gaze_vector(yaw_degrees=0, pitch_degrees=0) == (0.0, 0.0, 1.0)
