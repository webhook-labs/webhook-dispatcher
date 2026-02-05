use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Destination for webhook delivery.
///
/// An `Endpoint` describes *where* and *how* a webhook should be delivered.
/// It is a pure configuration object with no internal state.
///
/// Endpoints must be registered with the dispatcher before use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    /// Logical identifier for the endpoint.
    pub id: EndpointId,

    /// Target URL for webhook delivery.
    pub url: String,

    /// Maximum time allowed for a single delivery attempt.
    pub timeout: Duration,

    /// Maximum number of retry attempts after the initial attempt.
    pub max_retries: u32,

    /// Maximum number of concurrent deliveries allowed for this endpoint.
    pub max_concurrent: usize,

    /// Optional secret for HMAC signing.
    pub secret: Option<Vec<u8>>,

    /// Whether to include timestamp in signatures.
    pub include_timestamp: bool,

    /// Signature header name.
    pub signature_header: String,

    /// Timestamp header name.
    pub timestamp_header: String,

    /// Max requests per second for this endpoint (rate limit).
    pub max_rps: Option<u32>,

    /// Burst capacity for this endpoint (rate limit).
    pub burst: Option<u32>,

    /// Optional tenant identifier for multi-tenant isolation.
    pub tenant_id: Option<TenantId>,

    /// Optional per-endpoint retry overrides.
    pub retry_base_ms: Option<u64>,
    pub retry_max_ms: Option<u64>,
    pub retry_jitter_ms: Option<u64>,
}

impl Endpoint {
    /// Create a new endpoint with default delivery settings.
    ///
    /// Defaults:
    /// - timeout: 5 seconds
    /// - max_retries: 3
    /// - max_concurrent: 10
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: EndpointId(id.into()),
            url: url.into(),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            max_concurrent: 10,
            secret: None,
            include_timestamp: true,
            signature_header: "X-Webhook-Signature".to_string(),
            timestamp_header: "X-Webhook-Timestamp".to_string(),
            max_rps: None,
            burst: None,
            tenant_id: None,
            retry_base_ms: None,
            retry_max_ms: None,
            retry_jitter_ms: None,
        }
    }

    /// Set a custom timeout for delivery attempts.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of retry attempts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the maximum number of concurrent deliveries.
    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent = max_concurrent;
        self
    }

    /// Set a secret for HMAC signing.
    pub fn with_secret(mut self, secret: impl Into<Vec<u8>>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// Enable or disable timestamped signatures.
    pub fn with_timestamped_signatures(mut self, include: bool) -> Self {
        self.include_timestamp = include;
        self
    }

    /// Customize signature header.
    pub fn with_signature_header(mut self, header: impl Into<String>) -> Self {
        self.signature_header = header.into();
        self
    }

    /// Customize timestamp header.
    pub fn with_timestamp_header(mut self, header: impl Into<String>) -> Self {
        self.timestamp_header = header.into();
        self
    }

    /// Set rate limit (requests per second) and burst capacity.
    pub fn with_rate_limit(mut self, max_rps: u32, burst: u32) -> Self {
        self.max_rps = Some(max_rps);
        self.burst = Some(burst);
        self
    }

    /// Assign this endpoint to a tenant.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(TenantId(tenant_id.into()));
        self
    }

    /// Override retry parameters for this endpoint.
    pub fn with_retry_policy(mut self, base_ms: u64, max_ms: u64, jitter_ms: u64) -> Self {
        self.retry_base_ms = Some(base_ms);
        self.retry_max_ms = Some(max_ms);
        self.retry_jitter_ms = Some(jitter_ms);
        self
    }
}

/// Logical event to be delivered.
///
/// The dispatcher treats the payload as opaque bytes.
/// Serialization and schema management are the caller's responsibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Logical identifier for the event.
    pub id: EventId,

    /// Serialized event payload.
    pub payload: Vec<u8>,

    /// Optional tenant identifier for multi-tenant isolation.
    pub tenant_id: Option<TenantId>,
}

impl Event {
    /// Create a new event with the given ID and payload.
    pub fn new(id: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id: EventId(id.into()),
            payload: payload.into(),
            tenant_id: None,
        }
    }

    /// Assign this event to a tenant.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(TenantId(tenant_id.into()));
        self
    }
}

/// Unique identifier for an endpoint.
///
/// This is a strongly-typed wrapper to avoid accidental mixing
/// of endpoint IDs with other string identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointId(pub String);

/// Unique identifier for an event.
///
/// This is a strongly-typed wrapper to avoid accidental mixing
/// of event IDs with other string identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub String);

/// Unique identifier for a tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub String);

/// Idempotency key for an event delivery.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub event_id: EventId,
    pub endpoint_id: EndpointId,
    pub tenant_id: Option<TenantId>,
}

impl IdempotencyKey {
    pub fn new(event_id: EventId, endpoint_id: EndpointId, tenant_id: Option<TenantId>) -> Self {
        Self { event_id, endpoint_id, tenant_id }
    }
}

/// Dead-letter queue entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub key: IdempotencyKey,
    pub payload: Vec<u8>,
    pub endpoint_id: EndpointId,
    pub failure: String,
    pub created_at_secs: u64,
}

/// Delivery lifecycle status for an event + endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryStatus {
    Queued,
    Retrying,
    Delivered,
    Failed,
    Dropped,
    Dlq,
}

/// Delivery status details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryState {
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub last_updated_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject new work when queue is full.
    DropNewest,
    /// Block until capacity is available.
    Block,
    /// Best-effort drop of oldest overflow entries.
    DropOldest,
    /// Persist overflow to storage for later replay.
    SpillToStorage,
}
