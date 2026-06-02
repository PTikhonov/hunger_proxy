# Ingest Server

Async FastAPI service that receives detection/bestshot messages from the video analytics server and appends normalized events to Redis Streams.

## Run

```powershell
docker compose up --build
```

## Endpoints

- `GET /health` - service health.
- `POST /video-detector/frame` - preferred JSON or multipart ingestion endpoint.
- `POST /context_pers` - legacy-compatible ingestion endpoint name.

 `POST http://192.168.1.10:8000/video-detector/frame`

## Redis Stream

Default stream: `stream:detections`.

Each entry contains flat searchable fields and a JSON `payload` field:

```text
event_type=person_detected
event_id=<uuid>
camera_id=<camera id>
event_timestamp=<timestamp from video server>
received_at=<server timestamp>
payload=<normalized JSON>
```

For multipart requests, binary images are stored outside the stream in short-lived Redis keys. `normalized` remains available for fast embedding extraction, while both `normalized` and `photo`/`full_frame` get ffupload URLs immediately:

```json
{
  "media": {
    "normalized": {
      "filename": "normalized.jpg",
      "content_type": "image/jpeg",
      "size": 204800,
      "media_type": "normalized",
      "redis_key": "media:detection:<event_id>:normalized",
      "ttl_seconds": 120,
      "upload_url": "http://192.168.1.25:3333/uploads/<event_id>_norm.jpg",
      "public_url": "http://192.168.1.25:3333/uploads/<event_id>_norm.jpg",
      "upload_status": "pending"
    },
    "photo": {
      "filename": "full_frame.jpg",
      "content_type": "image/jpeg",
      "size": 1048576,
      "media_type": "full_frame",
      "redis_key": "media:detection:<event_id>:full_frame",
      "ttl_seconds": 120,
      "upload_url": "http://192.168.1.25:3333/uploads/<event_id>_full.jpg",
      "public_url": "http://192.168.1.25:3333/uploads/<event_id>_full.jpg",
      "upload_status": "pending"
    }
  }
}
```

The service also writes `stream:media-save-jobs`; `media-uploader` reads it and saves images to ffupload with `PUT`.


## Logging

`LOG_LEVEL` is configured through environment variables and supports only:

- `INFO` - default operational logs.
- `DEBUG` - logs the full incoming request; multipart files are logged as metadata to avoid binary output in logs.

```yaml
environment:
  LOG_LEVEL: DEBUG
```
