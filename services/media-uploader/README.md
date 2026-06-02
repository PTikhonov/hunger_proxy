# Media Uploader

Async worker that reads `stream:media-save-jobs`, loads temporary image bytes from Redis, and saves them to ffupload/WebDAV using HTTP `PUT`.

The `ingest-server` generates final URLs immediately and adds them to `stream:detections`; this worker only makes those URLs become real files.

By default, uploaded Redis media keys are not deleted immediately. They expire by TTL so `visitor-state-processor` can still read `normalized` for extraction even if upload finishes first.
