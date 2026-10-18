# State Visualizer

Debug-only web UI for visual inspection of `redis-hot-state`.

It renders active identities in three vertical columns:

- silhouettes
- faces
- matched identities

The UI polls `/api/state` and shows all Redis hash fields plus normalized images from `media_links` in chronological order.

Opening a silhouette full frame displays it in a modal with an SVG overlay. When
`gaze_annotation:<source_event_id>` is available, the overlay shows the head bbox,
estimated head distance, gaze direction, shelf polygons, and ray-plane intersection
markers calculated for that exact observation.
