# Webhook Dispatcher (Rust)

An in-process webhook delivery engine with fairness, retries, DLQ, signatures, and pluggable durability.

## Quickstart 

```rust
use webhook_dispatcher::{Dispatcher, DispatcherConfig, Endpoint, Event};

#[tokio::main]
async fn main() {
    let dispatcher = Dispatcher::new(DispatcherConfig::default());

let endpoint = Endpoint::new("orders", "https://example.com/webhook")
    .with_secret(b"supersecret")
    .with_tenant_id("tenant_a")
    .with_rate_limit(100, 200);

    dispatcher.register_endpoint(endpoint).await;

let event = Event::new("evt_123", r#"{"id":123}"#.as_bytes())
    .with_tenant_id("tenant_a");
let _ = dispatcher.dispatch(event, vec!["orders".into()]).await;
}
```

## Production Checklist

1. Enable real HTTP delivery: `--features http`
2. Use a durable backend for restarts: Redis or Postgres
3. Choose an overflow policy: `Block` or `SpillToStorage`
4. Set rate limits (endpoint + tenant) to protect downstreams
5. Use signature verification on the receiver side

## Receiver Verification

```rust
use webhook_dispatcher::verify_webhook_request;

let headers = vec![
    ("X-Webhook-Signature", "abcd..."),
    ("X-Webhook-Timestamp", "1700000000"),
];

verify_webhook_request(
    headers.iter().map(|(k, v)| (*k, *v)),
    payload_bytes,
    b"supersecret",
    "X-Webhook-Signature",
    "X-Webhook-Timestamp",
    300,
    now_secs,
)?;
```

## Features

- Fair scheduling with sharded queues
- Retries with jitter + DLQ
- Per-endpoint retry policy overrides
- HMAC signatures and timestamp support
- Per-endpoint rate limiting
- Multi-tenant isolation (tenant id on endpoints/events)
- Pluggable storage (in-memory, Redis, Postgres)
- Metrics and tracing feature flags

## Optional Features

```bash
cargo build --features http
cargo build --features redis
cargo build --features postgres
cargo build --features metrics
cargo build --features tracing
```

## Durable Storage Backends

### In-Memory (default)
Fast and simple, but not durable across restarts.

```rust
let dispatcher = Dispatcher::new(DispatcherConfig::default());
```

### Redis
```rust
use std::sync::Arc;
use webhook_dispatcher::{Dispatcher, DispatcherConfig, RedisStorage};

let client = redis::Client::open("redis://127.0.0.1/")?;
let storage = Arc::new(RedisStorage::new(client, "webhook_dispatcher"));
let dispatcher = Dispatcher::new_with_storage(DispatcherConfig::default(), storage).await;
```

### Postgres
```rust
use std::sync::Arc;
use webhook_dispatcher::{Dispatcher, DispatcherConfig, PostgresStorage};

let (client, connection) =
    tokio_postgres::connect("host=localhost user=postgres", tokio_postgres::NoTls).await?;
tokio::spawn(connection);

let storage = Arc::new(PostgresStorage::new(client).await?);
let dispatcher = Dispatcher::new_with_storage(DispatcherConfig::default(), storage).await;
```

## Common Recipes

### Set Overflow Policy
```rust
use webhook_dispatcher::{DispatcherConfig, OverflowPolicy};

let mut cfg = DispatcherConfig::default();
cfg.overflow_policy = OverflowPolicy::Block;
```

### Per-Endpoint Retry Overrides
```rust
use webhook_dispatcher::Endpoint;

let endpoint = Endpoint::new("orders", "https://example.com/webhook")
    .with_retry_policy(200, 2_000, 50);
```

### Tenant Rate Limits
```rust
use webhook_dispatcher::{Dispatcher, TenantId};

dispatcher
    .set_tenant_rate_limit(TenantId("tenant_a".to_string()), 300, 600)
    .await;
```

### DLQ Replay
```rust
use webhook_dispatcher::IdempotencyKey;

let replayed = dispatcher.replay_dlq_all().await;
let ok = dispatcher.replay_dlq_entry(&IdempotencyKey::new(
    "evt_123".into(),
    "orders".into(),
    Some("tenant_a".into()),
)).await;
```

### Delivery Status Queries
```rust
use webhook_dispatcher::IdempotencyKey;

let status = dispatcher.delivery_status(&IdempotencyKey::new(
    "evt_123".into(),
    "orders".into(),
    Some("tenant_a".into()),
)).await;
```

## CLI/Usage Notes

- This is a library; you call it from your app.
- To test real delivery, enable `http` and point to a real URL.

## Example Configs (Different Scales)

### Small (dev / side-project)
```rust
use webhook_dispatcher::DispatcherConfig;

let cfg = DispatcherConfig {
    max_in_flight: 50,
    shard_queue_size: 200,
    shard_count: 8,
    worker_count: 2,
    ..Default::default()
};
```

### Medium (startup)
```rust
use webhook_dispatcher::DispatcherConfig;

let cfg = DispatcherConfig {
    max_in_flight: 500,
    shard_queue_size: 2_000,
    shard_count: 32,
    worker_count: 8,
    ..Default::default()
};
```

### Large (high throughput)
```rust
use webhook_dispatcher::DispatcherConfig;

let cfg = DispatcherConfig {
    max_in_flight: 5_000,
    shard_queue_size: 10_000,
    shard_count: 128,
    worker_count: 32,
    ..Default::default()
};
```

## Architecture Diagram

```text
         ┌───────────────┐
         │  Dispatcher   │
         └──────┬────────┘
                │ dispatch(event)
         ┌──────▼────────┐
         │ Sharded Queues│  (fair scheduling)
         └──────┬────────┘
                │
         ┌──────▼────────┐
         │  Scheduler    │  (retry + jitter + DLQ)
         └──────┬────────┘
                │
         ┌──────▼────────┐
         │   Workers     │  (rate limit + HTTP)
         └──────┬────────┘
                │
         ┌──────▼────────┐
         │   Endpoints   │
         └───────────────┘
```

## Troubleshooting

### I’m not seeing webhooks delivered
- Ensure you ran with `--features http`.
- Check endpoint URL and network access.
- If you see DLQ entries, replay them or check the status.

### I’m getting backpressure errors
- Increase `shard_queue_size` or `max_in_flight`.
- Use `OverflowPolicy::Block` for safer behavior.

### Retries don’t seem to happen
- Confirm `max_retries` is set on the endpoint.
- Verify that the failure is retryable (4xx is non-retryable).

### Signature verification fails
- Make sure the secret matches.
- Confirm the timestamp is within `max_age_secs`.
- Validate header names match your endpoint config.

### Redis/Postgres durability not working
- Confirm the feature flag is enabled (`redis` or `postgres`).
- Ensure the storage backend is used via `new_with_storage`.
- Check connection details and permissions.

## Prometheus Metrics Example

Add the exporter in your app (not required by the library):

```toml
# Cargo.toml
metrics-exporter-prometheus = "0.15"
```

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

let _handle = PrometheusBuilder::new().install().unwrap();
```

Then run with `--features metrics` and scrape the metrics endpoint exposed by your app.

## Notes

- Default mode is in-memory (fast, not durable across restarts).
- Use `new_with_storage` for durable backends.
