# api-sentinel-archiver

Dedicated archive and retention process: moves scan evidence and artifacts
into long-term storage and enforces tenant retention policies.

## Status: vendored build, decoupling pending

This is the simplest service to decouple: the storage module has no API
imports, only shared models. The runtime is vendored under `server/` so the
image builds and the archiver loop runs today; extracting the
shared-contracts package remains the next stage.

## Run

```bash
docker build -t api-sentinel/archiver:local .
docker run --rm api-sentinel/archiver:local
```

Entry point: `python -m server.services.archiver_service` (needs Postgres
and the standard API environment variables; archives under `/app/data`).
