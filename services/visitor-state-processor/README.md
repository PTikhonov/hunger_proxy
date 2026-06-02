# Visitor State Processor

Async worker that reads raw detection events from Redis Streams, extracts embeddings through `extraction-api`, resolves typed identities in memory, updates Redis hot state, and emits identity observations.

## Flow

```text
stream:detections
  -> visitor-state-processor
      -> GET normalized image from Redis key
      -> POST image to extraction-api
      -> resolve typed identity in RAM
      -> update Redis hot state
      -> XADD stream:identity-observations
```

## Important Defaults

- Input stream: `stream:detections`
- Output stream: `stream:identity-observations`
- Consumer group: `visitor-state-processor`
- Streams Redis: `redis://redis-streams:6379/0`
- Hot state Redis: `redis://redis-hot-state:6379/0`
- Normalized media field: `normalized`
- Extraction API: `http://host.docker.internal:18666/v2`
- Face extraction request: `detector=face`, `attributes=face_emben` plus optional `face_age` and `face_gender`
- Silhouette extraction request: `detector=body`, `attributes=body_emben` plus optional `body_age_gender`

The worker expects `ingest-server` to put `media.normalized.redis_key` into the detection payload.

Embeddings are kept in separate in-memory namespaces by detection type, so face embeddings are matched only with face embeddings, and silhouette embeddings are matched only with silhouette embeddings.

Each `identity:*` hash in Redis Hot State stores `media_links` as a JSON array. Links come from detection payload media entries and are kept for the full lifetime of the identity hot-state key.

Redis Hot State uses separate hash prefixes for aggregate and scoped counters:

- `identity:<identity_id>` stores aggregate identity state, latest attributes, media links, `camera_ids`, and `category_ids`
- `identity_camera:<identity_id>:<camera_id>` stores presence counters for the identity on a single camera
- `identity_category:<identity_id>:<category_id>` stores category-scoped counters when category data is present in detection payloads

Identity matching thresholds are configured separately:

- `FACE_IDENTITY_CONFIDENCE_THRESHOLD`
- `SILHOUETTE_IDENTITY_CONFIDENCE_THRESHOLD`

Age and gender extraction are configured separately by detection type:

- `FACE_EXTRACTION_NEED_AGE`
- `FACE_EXTRACTION_NEED_GENDER`
- `SILHOUETTE_EXTRACTION_NEED_AGE`
- `SILHOUETTE_EXTRACTION_NEED_GENDER`
