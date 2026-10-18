from app.geometry import gaze_vector, project_direction, project_point, ray_intersects_shelf


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


def test_project_point_uses_camera_calibration() -> None:
    camera = {
        "frame_width": 1280,
        "frame_height": 720,
        "focal_length_in_mm": 2.8,
        "sensor_width_in_mm": 4.92,
        "sensor_height_in_mm": 2.77,
    }
    point = (1.8852220982142855, -0.18260565476190477, 4.43)
    assert project_point(camera, point) == (950.0, 330.0)


def test_project_direction_returns_pixel_delta_to_vanishing_point() -> None:
    camera = {
        "frame_width": 1280,
        "frame_height": 720,
        "focal_length_in_mm": 2.8,
        "sensor_width_in_mm": 4.92,
        "sensor_height_in_mm": 2.77,
    }
    direction = (0.2677940801436467, -0.2651354262434081, 0.9262772459640497)
    projected = project_direction(camera, direction)
    assert abs(projected[0] - 210.602) < 0.001
    assert abs(projected[1] + 208.323) < 0.001
