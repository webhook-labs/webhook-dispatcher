//! A single-process webhook delivery engine.
//!
//! This crate provides a **bounded, in-memory, best-effort**
//! webhook dispatcher intended for embedded / in-house usage.
//!
//! ## Guarantees
//! - Bounded resource usage
//! - Explicit backpressure
//! - Per-endpoint isolation
//! - Best-effort, at-least-once delivery
//!
//! ## Non-Guarantees
//! - Durability across restarts
//! - Exactly-once delivery
//! - Distributed coordination
//! - Persistent storage
//!
//! This crate is intentionally **not a hosted service**.
//! It exists to expose the *real* complexity of webhook delivery
//! inside a single process.

mod dispatcher;
mod worker;
mod types;
mod error;
mod signing;
mod storage;

#[cfg(feature = "redis")]
mod storage_redis;

#[cfg(feature = "postgres")]
mod storage_postgres;

pub use dispatcher::{Dispatcher, DispatcherConfig};
pub use types::{Endpoint, EndpointId, Event, EventId, DeliveryState, DeliveryStatus, IdempotencyKey, DlqEntry, OverflowPolicy, TenantId};
pub use error::{
    DispatchError,
    DeliveryOutcome,
    DropReason,
    FailureReason,
};
pub use storage::{Storage, InMemoryStorage};
pub use signing::{
    compute_signature,
    verify_signature,
    is_timestamp_fresh,
    parse_signature_headers,
    ParsedSignature,
    verify_webhook_request,
    VerificationError,
};
pub use worker::RateLimiterStats;

#[cfg(feature = "redis")]
pub use storage_redis::RedisStorage;

#[cfg(feature = "postgres")]
pub use storage_postgres::PostgresStorage;
