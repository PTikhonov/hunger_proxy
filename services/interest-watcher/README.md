# Interest Watcher

Consumes matcher-enriched silhouette events from `stream:person-events` in its own consumer group.
`matcher` writes the `person_id` into this event only after it has committed the
identity-to-person link, so interest calculation does not race Redis hot-state.
For every observation it loads the exact normalized silhouette crop, detects the head and
`head_headpose_unconstrained` through Extraction API, translates the head bbox to full-frame
coordinates using the silhouette bbox, and tests the gaze ray against every configured shelf.

Geometry is read from DB 1 of `redis-hot-state`; runtime interest is written to DB 0 directly
into the silhouette's `person:*` hash. A shelf uses these fields:

```text
interest:<camera_id>:<shelf_id>:intersects
interest:<camera_id>:<shelf_id>:total_seconds
interest:<camera_id>:<shelf_id>:last_observed_epoch
interest:<camera_id>:<shelf_id>:updated_at
interest:<camera_id>:<shelf_id>:detail
interest:<camera_id>:<shelf_id>:consecutive_misses
```

Time is accumulated when an intersecting observation continues an active shelf interval and
is separated by no more than `INTEREST_GAP_SECONDS`. A temporary loss of intersection keeps
the interval active; it is reset only on the third consecutive miss by default. Configure the
threshold with `INTEREST_MISSES_BEFORE_RESET`. The update is an atomic Redis Lua operation, so
matcher updates to other person fields cannot be overwritten.

Category interest is also recorded atomically in the same `person:*` hash:

```text
interest-category:<camera_id>:<category_id>:total_seconds
interest-category:<location_id>:<category_id>:total_seconds
```

The second key is the canonical aggregate for reacting to category interest at a store. When
its value crosses a context threshold from below, the same Redis Lua operation creates the
idempotency/cooldown key
`interest-threshold:cooldown:<person_id>:<location_id>:<category_id>:<context_id>:<rule_id>:<threshold>`
and appends an `interest_threshold_crossed` record to `stream:interest-threshold-events` in
Redis Hot State. A future reaction consumer must read that stream from Redis Hot State, not
Redis Streams. The event includes the total before and after crossing, camera, matched category,
context/rule/action, priority and rule revision.

If the temporary Redis image has expired, the worker falls back to `public_url`/`upload_url`.
When the image is absent from both locations, the unrecoverable observation is appended to
`stream:interest-watcher-dead-letter` and acknowledged in the source consumer group. Network
errors and HTTP 5xx responses remain pending and are retried; a permanent 404 is not retried.
An image for which Extraction API definitively detects no head is also written to DLQ and
acknowledged, because retrying the same immutable crop cannot change that result.
Transient failures are retried at most `INTEREST_MAX_RETRIES` times (default `5`) and then
also written to DLQ. New messages are read before a bounded batch of pending messages, so a
failed record cannot starve the consumer group.

Multiple heads from the same Extraction API response are filtered without additional API
calls. A candidate is discarded when either side is less than `MIN_HEAD_SIZE` (default `30`
pixels), `head_width / head_height` is greater than `MAX_HEAD_WIDTH_TO_HEIGHT_RATE` (default
`1`), `head_height / body_height` is greater than `MAX_HEAD_HEIGHT_TO_BODY_HEIGHT` (default
`0.7`), or `head_bbox_area / silhouette_crop_area` is greater than
`MAX_HEAD_TO_BODY_PROPORTION` (default `0.5`). The highest-score remaining candidate is used.

The normalized image must be an exact, unscaled crop. Therefore:

```text
head_left_full = silhouette_left_full + head_left_in_crop
head_top_full  = silhouette_top_full  + head_top_in_crop
```

The x/y conversion deliberately follows `intersection_v2/main.py` without multiplying by
the estimated distance. Head pose is converted to camera axes as
`(-sin(yaw)*cos(pitch), -sin(pitch), cos(yaw)*cos(pitch))`, where X points right,
Y points down and Z points away from the camera.

New geometry should send `viewing_direction`. Existing geometry remains supported through
the mapping `side=A -> normal`, `side=B -> opposite`, `side=AB -> both`. For a one-sided
shelf, interest is counted when the gaze approaches from the side opposite to the arrow
shown in hunger_head. `AB` continues to allow both directions.

For every successfully processed silhouette observation, the worker stores a temporary
`gaze_annotation:<source_event_id>` JSON value in Redis Hot State. It contains the
full-frame head bbox, estimated head distance, gaze vector, geometry revision, and shelf
polygons already projected to full-frame pixel coordinates. Its TTL matches
`PERSON_TTL_SECONDS`.
