# Matcher Service

Async worker that links new face identities with nearby silhouette identities.

## Flow

```text
stream:identity-observations
  -> matcher-service
      -> indexes silhouette observations by camera and event time
      -> waits for face observations with new_face=true
      -> finds silhouette candidates in [face_ts - 10s, face_ts + 2s]
      -> extracts face_emben from candidate silhouette normalized image
      -> compares with face embedding from observation payload
      -> writes person_id into both identity hashes
      -> XADD stream:person-events
```

Embeddings are not stored in Redis Hot State. Face embeddings are consumed from `stream:identity-observations`; face embeddings extracted from silhouette images are cached only in matcher temporary keys with TTL.
