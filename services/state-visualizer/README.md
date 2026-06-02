# State Visualizer

Debug-only web UI for visual inspection of `redis-hot-state`.

It renders active identities in three vertical columns:

- silhouettes
- faces
- matched identities

The UI polls `/api/state` and shows all Redis hash fields plus normalized images from `media_links` in chronological order.
