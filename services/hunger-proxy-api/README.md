# Hunger Proxy API

Internal configuration API for camera calibration and shelf geometry. `hunger_head` is the source of truth; this service validates versioned full-camera snapshots and stores them in Redis DB 1 of `redis-hot-state`.

Runtime visitor state continues to use Redis DB 0. Geometry keys have no TTL.

## Run

```bash
docker compose up -d --build hunger-proxy-api
```

The API listens on container port `8008`, exposed as host port `8008`.

Set `HUNGER_PROXY_API_TOKEN` in the Compose environment to require:

```http
Authorization: Bearer <token>
```

An empty token disables authentication and is intended only for trusted development networks.

## Redis layout

```text
DB 0  visitor hot state (identity:*, person:*, matcher_*)
DB 1  camera geometry
```

DB 1 keys:

```text
hunger:geometry:camera:<camera_id>  Redis Hash containing revision, checksum and payload
hunger:geometry:cameras             Redis Set containing known camera IDs
hunger:geometry:changed             Pub/Sub invalidation channel
```

Camera geometry is stored as one complete payload. Camera calibration and shelves are never updated independently.

## API required from `hunger_head`

`hunger_head` should create an outbox record in the same PostgreSQL transaction as every camera/shelf change. Its worker sends the complete current snapshot of the affected camera. Delivery is at least once; revisions and checksums make requests idempotent.

### Create or replace camera geometry

```http
PUT /internal/v1/cameras/{camera_id}/geometry
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "schema_version": 1,
  "camera_id": 32,
  "revision": 184,
  "updated_at": "2026-10-10T12:15:24Z",
  "camera": {
    "focal_length_in_mm": 2.8,
    "sensor_width_in_mm": 4.92,
    "sensor_height_in_mm": 2.77,
    "frame_width": 1920,
    "frame_height": 1080
  },
  "shelves": [
    {
      "shelf_id": 7,
      "name": "Shelf 1",
      "category_id": 15,
      "category_2_id": null,
      "side": "A",
      "side_2": "",
      "viewing_direction": "normal",
      "points": [
        [-1.12, 0.34, 2.8],
        [0.42, 0.35, 2.8],
        [0.41, -0.17, 2.8],
        [-1.11, -0.18, 2.8]
      ]
    }
  ]
}
```

The `points` values are camera-coordinate metres. Pickled `point1`, `point2`, `plane`, or other Python objects must not be sent.

`viewing_direction` selects the permitted approach direction. The polygon point order defines
`normal = (points[0] - points[2]) x (points[0] - points[1])`, matching the calculation in
`intersection_v2`. `normal` means the gaze ray must point against that normal, `opposite`
means it must point along it, and `both` disables the side check. Reverse the polygon point
order to reverse its normal. If omitted, `both` is used for backward compatibility.
During migration, `interest-watcher` also understands legacy `side`: `A` maps to `normal`,
`B` maps to `opposite`, and `AB` maps to `both`. For a one-sided shelf this makes the
permitted approach direction opposite to the arrow shown in hunger_head. New senders should
always provide `viewing_direction` explicitly.

Responses:

- `200`: applied or idempotent replay. Body status is `created`, `updated`, or `unchanged`.
- `409 stale_revision`: Redis already contains a newer revision.
- `409 revision_checksum_conflict`: the same revision was sent with different content.
- `422`: invalid contract or path/body camera mismatch.

When the last shelf is removed, send a normal `PUT` with `"shelves": []`. The camera remains available.

### Delete a camera

```http
DELETE /internal/v1/cameras/{camera_id}/geometry?revision=185
Authorization: Bearer <token>
```

Deletion writes a versioned tombstone instead of removing revision history. Delayed older requests therefore cannot resurrect the camera.

### Read one camera (diagnostics)

```http
GET /internal/v1/cameras/{camera_id}/geometry
Authorization: Bearer <token>
```

### List cameras (diagnostics and worker bootstrap)

```http
GET /internal/v1/cameras
GET /internal/v1/cameras?include_deleted=true
Authorization: Bearer <token>
```

### Health checks

```http
GET /health
GET /ready
```

## Revision rules for `hunger_head`

- Maintain a monotonically increasing geometry revision per camera.
- Increment it whenever camera calibration, frame dimensions, shelf coordinates, shelf categories, shelf side/direction, shelf creation, or shelf deletion changes.
- Retry the exact same serialized logical snapshot with the same revision until a successful response.
- Never reuse a revision for different content.
- Send a full snapshot, not a shelf delta.
- Periodically replay current snapshots to reconcile Redis after operational failures.

## Consumer behavior

After an accepted change the service publishes:

```json
{
  "event_type": "camera_geometry_updated",
  "camera_id": "32",
  "revision": 184
}
```

Deletion uses `camera_geometry_deleted`. `interest-watcher` should load DB 1 on startup, subscribe to the channel, reload the affected key after a notification, and periodically compare revisions because Redis Pub/Sub is only an invalidation signal.
