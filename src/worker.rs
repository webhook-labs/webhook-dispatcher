use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Semaphore, Mutex, RwLock};
use tokio::time::sleep;

use crate::error::{DeliveryOutcome, DropReason, FailureReason};
use crate::types::{Endpoint, EndpointId, Event, TenantId};
use crate::signing::{build_signature_headers, SignatureHeaders};
use tokio::time::Instant;

#[cfg(feature = "metrics")]
fn metric_inc(name: &'static str) {
    metrics::increment_counter!(name);
}

#[cfg(not(feature = "metrics"))]
fn metric_inc(_name: &'static str) {}

#[cfg(feature = "metrics")]
fn metric_inc_endpoint(name: &'static str, endpoint_id: &EndpointId) {
    metrics::increment_counter!(name, "endpoint" => endpoint_id.0.clone());
}

#[cfg(not(feature = "metrics"))]
fn metric_inc_endpoint(_name: &'static str, _endpoint_id: &EndpointId) {}

#[cfg(feature = "metrics")]
fn metric_inc_tenant(name: &'static str, tenant_id: &TenantId) {
    metrics::increment_counter!(name, "tenant" => tenant_id.0.clone());
}

#[cfg(not(feature = "metrics"))]
fn metric_inc_tenant(_name: &'static str, _tenant_id: &TenantId) {}

#[cfg(feature = "tracing")]
fn trace_event(message: &'static str) {
    tracing::info!(message);
}

#[cfg(not(feature = "tracing"))]
fn trace_event(_message: &'static str) {}

/// A unit of work consumed by workers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub event: Event,
    pub endpoint_id: EndpointId,
    pub attempt: u32,
}

/// Result of a single delivery attempt.
#[derive(Debug, Clone)]
pub struct DeliveryReport {
    pub task: Task,
    pub outcome: DeliveryOutcome,
    pub retry_after: Option<Duration>,
}

/// Shared, read-only context for all workers.
pub struct WorkerContext { 
    /// Global concurrency limiter.
    pub global_semaphore: Arc<Semaphore>,

    /// Per-endpoint concurrency limiters.
    pub endpoint_semaphores: RwLock<HashMap<EndpointId, Arc<Semaphore>>>,

    /// Immutable endpoint configuration.
    pub endpoint_configs: RwLock<HashMap<EndpointId, Arc<Endpoint>>>,

    /// Reports from workers to the scheduler.
    pub report_tx: mpsc::Sender<DeliveryReport>,

    /// Per-endpoint rate limit buckets.
    pub rate_limiters: RwLock<HashMap<EndpointId, Arc<Mutex<TokenBucket>>>>,

    /// Per-tenant rate limit buckets.
    pub tenant_rate_limiters: RwLock<HashMap<TenantId, Arc<Mutex<TokenBucket>>>>,

    /// Optional HTTP client for real delivery.
    #[cfg(feature = "http")]
    pub http_client: reqwest::Client,
}

/// Token bucket rate limiter.
#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub capacity: f64,
    pub tokens: f64,
    pub refill_per_sec: f64,
    pub last_refill_age_ms: u64,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_per_sec: u32) -> Self {
        let cap = capacity.max(1) as f64;
        Self {
            capacity: cap,
            tokens: cap,
            refill_per_sec: refill_per_sec.max(1) as f64,
            last_refill: Instant::now(),
        }
    }

    pub fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        let refill = elapsed * self.refill_per_sec;
        self.tokens = (self.tokens + refill).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn snapshot(&self) -> RateLimiterStats {
        let age = Instant::now().duration_since(self.last_refill).as_millis() as u64;
        RateLimiterStats {
            capacity: self.capacity,
            tokens: self.tokens,
            refill_per_sec: self.refill_per_sec,
            last_refill_age_ms: age,
        }
    }
}

/// Main worker loop.
///
/// Each worker:
/// - Pulls tasks from the shared queue
/// - Enforces global + per-endpoint limits
/// - Retries with exponential backoff
/// - Never holds permits during backoff
pub async fn worker_loop(
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    ctx: Arc<WorkerContext>,
) {
    loop {
        let task = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };

        let Some(task) = task else { break };

        let report = process_task(task, &ctx).await;
        let _ = ctx.report_tx.send(report).await;
    }
}

/// Process a single delivery attempt.
async fn process_task(
    mut task: Task,
    ctx: &WorkerContext,
) -> DeliveryReport {
    let endpoint = {
        let guard = ctx.endpoint_configs.read().await;
        match guard.get(&task.endpoint_id) {
            Some(e) => e.clone(),
            None => return DeliveryReport {
                task,
                outcome: DeliveryOutcome::Dropped(DropReason::Shutdown),
                retry_after: None,
            },
        }
    };

    let endpoint_semaphore = {
        let guard = ctx.endpoint_semaphores.read().await;
        match guard.get(&task.endpoint_id) {
            Some(s) => s.clone(),
            None => return DeliveryReport {
                task,
                outcome: DeliveryOutcome::Dropped(DropReason::Shutdown),
                retry_after: None,
            },
        }
    };

    if let Some(ref tenant_id) = task.event.tenant_id {
        if !try_consume_tenant_rate_limit(tenant_id, &ctx.tenant_rate_limiters).await {
            metric_inc("webhook.rate_limited.total");
            metric_inc_tenant("webhook.rate_limited.tenant", tenant_id);
            return DeliveryReport {
                task,
                outcome: DeliveryOutcome::Dropped(DropReason::Backpressure),
                retry_after: None,
            };
        }
    }

    if !try_consume_rate_limit(&task.endpoint_id, &ctx.rate_limiters).await {
        metric_inc("webhook.rate_limited.total");
        metric_inc_endpoint("webhook.rate_limited.endpoint", &task.endpoint_id);
        return DeliveryReport {
            task,
            outcome: DeliveryOutcome::Dropped(DropReason::Backpressure),
            retry_after: None,
        };
    }

    let max_retries = endpoint.max_retries;

    // Acquire permits per attempt
    let global_permit = match ctx.global_semaphore.try_acquire() {
        Ok(p) => p,
        Err(_) => return DeliveryReport {
            task,
            outcome: DeliveryOutcome::Dropped(DropReason::Backpressure),
            retry_after: None,
        },
    };

    let endpoint_permit = match endpoint_semaphore.try_acquire() {
        Ok(p) => p,
        Err(_) => {
            drop(global_permit);
            return DeliveryReport {
                task,
                outcome: DeliveryOutcome::Dropped(DropReason::Backpressure),
                retry_after: None,
            };
        }
    };

    // Simulated delivery
    let result = deliver(
        &endpoint,
        &task.event,
        #[cfg(feature = "http")]
        &ctx.http_client,
    ).await;

    // IMPORTANT: release permits BEFORE backoff
    drop(endpoint_permit);
    drop(global_permit);

    match result {
        Ok(_) => {
            metric_inc("webhook.delivery.success");
            trace_event("webhook.delivery.success");
            if let Some(ref tenant) = task.event.tenant_id {
                metric_inc_tenant("webhook.delivery.success.tenant", tenant);
            }
            DeliveryReport {
                task,
                outcome: DeliveryOutcome::Delivered,
                retry_after: None,
            }
        }

        Err(reason) => {
            if matches!(reason, FailureReason::ClientError) {
                return DeliveryReport {
                    task,
                    outcome: DeliveryOutcome::Failed(reason),
                    retry_after: None,
                };
            }

            if task.attempt >= max_retries {
                return DeliveryReport {
                    task,
                    outcome: DeliveryOutcome::Failed(FailureReason::MaxRetriesExceeded),
                    retry_after: None,
                };
            }

            task.attempt += 1;
            metric_inc("webhook.delivery.failure");
            trace_event("webhook.delivery.failure");
            if let Some(ref tenant) = task.event.tenant_id {
                metric_inc_tenant("webhook.delivery.failure.tenant", tenant);
            }
            DeliveryReport {
                task,
                outcome: DeliveryOutcome::Failed(reason),
                retry_after: Some(Duration::from_millis(0)),
            }
        }
    }
}

/// Simulated HTTP delivery (V1).
async fn deliver(
    endpoint: &Endpoint,
    event: &Event,
    #[cfg(feature = "http")] client: &reqwest::Client,
) -> Result<(), FailureReason> {
    let headers: SignatureHeaders = build_signature_headers(endpoint, event);

    #[cfg(feature = "http")]
    {
        let mut request = client.post(&endpoint.url).body(event.payload.clone());
        request = request.timeout(endpoint.timeout);

        if let Some((name, value)) = headers.signature_header {
            request = request.header(name, value);
        }
        if let Some((name, value)) = headers.timestamp_header {
            request = request.header(name, value);
        }
        request = request.header("Content-Type", "application/json");

        let response = request.send().await;
        return match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    Ok(())
                } else if resp.status().is_client_error() {
                    Err(FailureReason::ClientError)
                } else {
                    Err(FailureReason::RemoteError)
                }
            }
            Err(err) => {
                if err.is_timeout() {
                    Err(FailureReason::Timeout)
                } else {
                    Err(FailureReason::Network)
                }
            }
        };
    }

    #[cfg(not(feature = "http"))]
    {
        let _ = headers;
        sleep(Duration::from_millis(50)).await;
        if endpoint.timeout < Duration::from_millis(50) {
            return Err(FailureReason::Timeout);
        }
        Ok(())
    }
}

async fn try_consume_rate_limit(
    endpoint_id: &EndpointId,
    rate_limiters: &RwLock<HashMap<EndpointId, Arc<Mutex<TokenBucket>>>>,
) -> bool {
    let guard = rate_limiters.read().await;
    let Some(bucket) = guard.get(endpoint_id) else {
        return true;
    };
    let mut bucket = bucket.lock().await;
    bucket.try_take()
}

async fn try_consume_tenant_rate_limit(
    tenant_id: &TenantId,
    rate_limiters: &RwLock<HashMap<TenantId, Arc<Mutex<TokenBucket>>>>,
) -> bool {
    let guard = rate_limiters.read().await;
    let Some(bucket) = guard.get(tenant_id) else {
        return true;
    };
    let mut bucket = bucket.lock().await;
    bucket.try_take()
}

// Backoff is computed by the scheduler to keep config centralized.
