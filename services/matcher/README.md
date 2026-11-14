# Matcher Service

Async worker that links face identities with nearby silhouette identities and
records face-proven links between separate silhouette persons.

## Flow

```text
stream:identity-observations
  -> matcher-service
      -> indexes silhouette observations and marks unmatched silhouette persons as pending
      -> accepts a new face, or an existing face while pending silhouettes exist
      -> finds silhouette candidates in [face_ts - lookback, face_ts + lookahead]
      -> extracts face_emben from candidate silhouette normalized image
      -> compares with face embedding from observation payload
      -> for an unlinked face: writes person_id into both identity hashes
      -> for a linked face and a separate pending silhouette person:
         writes person_link:P1:P2 without changing either person_id
      -> XADD stream:person-events
```

Face embeddings are consumed from `stream:identity-observations`; embeddings
extracted from silhouette images are cached only in `matcher_silhouette_face_embedding:*`
temporary keys with TTL.

`person_link:*` is evidence of a confirmed relation, not a person merge. Both
`person:*` hashes retain their own interests and reaction eligibility. The same
related-person list is written to both hashes for analytics traversal.
