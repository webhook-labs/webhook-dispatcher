# Changelog

## 0.1.0
- Initial release with in-process webhook delivery.
- Fair sharded scheduling, retries with jitter, and DLQ.
- HMAC signing + receiver verification helpers.
- Per-endpoint and per-tenant rate limiting.
- Multi-tenant isolation.
- Pluggable storage (in-memory, Redis, Postgres).
- Delivery status tracking + replay APIs.
- Optional HTTP delivery, metrics, and tracing features.
