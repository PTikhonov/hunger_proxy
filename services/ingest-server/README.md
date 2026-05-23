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


## Logging

`LOG_LEVEL` is configured through environment variables and supports only:

- `INFO` - default operational logs.
- `DEBUG` - logs the full incoming request, including headers and body.

```yaml
environment:
  LOG_LEVEL: DEBUG
```
