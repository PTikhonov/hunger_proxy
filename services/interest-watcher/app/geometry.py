from __future__ import annotations

import math
from typing import Any, Sequence

EPSILON = 1e-9


def sub(a: Sequence[float], b: Sequence[float]) -> tuple[float, float, float]:
    return a[0] - b[0], a[1] - b[1], a[2] - b[2]


def add_scaled(a: Sequence[float], b: Sequence[float], scale: float) -> tuple[float, float, float]:
    return a[0] + b[0] * scale, a[1] + b[1] * scale, a[2] + b[2] * scale


def dot(a: Sequence[float], b: Sequence[float]) -> float:
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


def cross(a: Sequence[float], b: Sequence[float]) -> tuple[float, float, float]:
    return (
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    )


def normalize(vector: Sequence[float]) -> tuple[float, float, float]:
    length = math.sqrt(dot(vector, vector))
    if length <= EPSILON:
        raise ValueError("zero-length vector")
    return vector[0] / length, vector[1] / length, vector[2] / length


def plane_normal(points: Sequence[Sequence[float]]) -> tuple[float, float, float]:
    # Same operand order as intersection_v2/main.py.
    return normalize(cross(sub(points[0], points[2]), sub(points[0], points[1])))


def gaze_vector(yaw_degrees: float, pitch_degrees: float, roll_degrees: float = 0.0) -> tuple[float, float, float]:
    """Convert Extraction API head pose to the camera-coordinate forward vector.

    Camera coordinates use X to the image right, Y down and Z away from the
    camera. Extraction API's image axes are inverted relative to these camera
    axes, so invert yaw and pitch while keeping a frontal pose along +Z.
    Roll rotates around the forward axis and does not change that axis.
    """
    del roll_degrees
    yaw = math.radians(yaw_degrees)
    pitch = math.radians(pitch_degrees)
    return normalize(
        (
            -math.sin(yaw) * math.cos(pitch),
            -math.sin(pitch),
            math.cos(yaw) * math.cos(pitch),
        )
    )


def head_point(camera: dict[str, Any], bbox: Sequence[float], head_height_meters: float) -> tuple[float, float, float]:
    left, top, right, bottom = (float(value) for value in bbox)
    bbox_height = bottom - top
    if bbox_height <= 0:
        raise ValueError("head bbox height must be positive")
    frame_width = float(camera["frame_width"])
    frame_height = float(camera["frame_height"])
    focal = float(camera["focal_length_in_mm"])
    sensor_width = float(camera["sensor_width_in_mm"])
    sensor_height = float(camera["sensor_height_in_mm"])
    object_height_on_sensor = sensor_height * bbox_height / frame_height
    z = head_height_meters * focal / object_height_on_sensor
    center_x = left + (right - left) / 2.0
    center_y = top + (bottom - top) / 2.0
    # Deliberately no multiplication by z: this is the requested reference behavior.
    x = (sensor_width * (center_x - frame_width / 2.0) / frame_width) / focal
    y = (sensor_height * (center_y - frame_height / 2.0) / frame_height) / focal
    return x, y, z


def ray_intersects_shelf(
    origin: Sequence[float],
    direction: Sequence[float],
    points: Sequence[Sequence[float]],
    viewing_direction: str = "both",
) -> tuple[bool, tuple[float, float, float] | None]:
    if len(points) < 3:
        return False, None
    normal = plane_normal(points)
    denominator = dot(normal, direction)
    if abs(denominator) <= EPSILON:
        return False, None
    if viewing_direction == "normal" and denominator >= -EPSILON:
        return False, None
    if viewing_direction == "opposite" and denominator <= EPSILON:
        return False, None
    t = dot(sub(points[0], origin), normal) / denominator
    if t <= EPSILON:
        return False, None
    intersection = add_scaled(origin, direction, t)
    return point_in_polygon_3d(intersection, points, normal), intersection


def point_in_polygon_3d(point: Sequence[float], points: Sequence[Sequence[float]], normal: Sequence[float]) -> bool:
    # Drop the dominant normal axis, avoiding the XY-only limitation of the prototype.
    dropped_axis = max(range(3), key=lambda index: abs(normal[index]))
    projected_polygon = [tuple(value for index, value in enumerate(vertex) if index != dropped_axis) for vertex in points]
    projected_point = tuple(value for index, value in enumerate(point) if index != dropped_axis)
    inside = False
    px, py = projected_point
    previous = projected_polygon[-1]
    for current in projected_polygon:
        x1, y1 = previous
        x2, y2 = current
        if _point_on_segment(px, py, x1, y1, x2, y2):
            return True
        crosses = (y1 > py) != (y2 > py)
        if crosses and px < (x2 - x1) * (py - y1) / (y2 - y1) + x1:
            inside = not inside
        previous = current
    return inside


def _point_on_segment(px: float, py: float, x1: float, y1: float, x2: float, y2: float) -> bool:
    area = (px - x1) * (y2 - y1) - (py - y1) * (x2 - x1)
    if abs(area) > EPSILON:
        return False
    return min(x1, x2) - EPSILON <= px <= max(x1, x2) + EPSILON and min(y1, y2) - EPSILON <= py <= max(y1, y2) + EPSILON
