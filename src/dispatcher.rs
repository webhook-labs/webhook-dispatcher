use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::cmp::Ordering as CmpOrdering;
use std::time::Duration;

use tokio::sync::{mpsc, Semaphore, Mutex, RwLock, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::error::DispatchError;
use crate::storage::{InMemoryStorage, Storage};

#[cfg(feature = "metrics")]
fn metric_inc(name: &'static str) {
    metrics::increment_counter!(name);
}

#[cfg(not(feature = "metrics"))]
fn metric_inc(_name: &'static str) {}

#[cfg(feature = "tracing")]
fn trace_event(message: &'static str) {
    tracing::info!(message);
}

#[cfg(not(feature = "tracing"))]
fn trace_event(_message: &'static str) {}
use crate::types::{Endpoint, EndpointId, Event, IdempotencyKey, DlqEntry, DeliveryState, DeliveryStatus, OverflowPolicy, TenantId};
use crate::worker::{worker_loop, Task, WorkerContext, DeliveryReport, TokenBucket, RateLimiterStats};

#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    pub max_in_flight: usize,
    pub shard_queue_size: usize,
    pub shard_count: usize,
    pub worker_count: usize,
    pub dlq_capacity: usize,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub retry_jitter_ms: u64,
    pub overflow_policy: OverflowPolicy,
    pub overflow_buffer_size: usize,
    pub tenant_max_rps: Option<u32>,
    pub tenant_burst: Option<u32>,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        let worker_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            max_in_flight: 100,
            shard_queue_size: 1_000,
            shard_count: 32,
            worker_count,
            dlq_capacity: 10_000,
            retry_base_ms: 100,
            retry_max_ms: 5_000,
            retry_jitter_ms: 50,
            overflow_policy: OverflowPolicy::DropNewest,
            overflow_buffer_size: 1_000,
            tenant_max_rps: None,
            tenant_burst: None,
        }
    }
}

pub struct Dispatcher {
    ready_tx: Option<mpsc::Sender<Task>>,
    is_running: Arc<AtomicBool>,
    worker_handles: Vec<JoinHandle<()>>,
    scheduler_handle: Option<JoinHandle<()>>,
    ctx: Arc<WorkerContext>,
    shard_senders: Vec<mpsc::Sender<Task>>,
    overflow_buffers: Arc<Vec<Mutex<VecDeque<Task>>>>,
    notify: Arc<Notify>,
    idempotency: Arc<RwLock<HashSet<IdempotencyKey>>>,
    dlq: Arc<Mutex<VecDeque<DlqEntry>>>,
    storage: Arc<dyn Storage>,
    status: Arc<RwLock<HashMap<IdempotencyKey, DeliveryState>>>,
    config: DispatcherConfig,
}

impl Dispatcher {
    pub fn new(config: DispatcherConfig) -> Self {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        Self::new_with_storage_inner(config, storage)
    }

    pub async fn new_with_storage(config: DispatcherConfig, storage: Arc<dyn Storage>) -> Self {
        let dispatcher = Self::new_with_storage_inner(config, storage.clone());
        let pending = storage.load_pending().await;
        for task in pending {
            let shard_index = shard_for_endpoint(&task.endpoint_id, dispatcher.shard_senders.len());
            if let Some(tx) = dispatcher.shard_senders.get(shard_index) {
                let _ = tx.send(task).await;
                dispatcher.notify.notify_one();
            }
        }

        let status_map = storage.load_status().await;
        if !status_map.is_empty() {
            let mut guard = dispatcher.status.write().await;
            for (key, state) in status_map {
                if state.status == DeliveryStatus::Delivered {
                    dispatcher.idempotency.write().await.insert(key.clone());
                }
                guard.insert(key, state);
            }
        }
        dispatcher
    }

    fn new_with_storage_inner(config: DispatcherConfig, storage: Arc<dyn Storage>) -> Self {
        let shard_count = config.shard_count.max(1);
        let (ready_tx, ready_rx) = mpsc::channel(config.shard_queue_size.max(1));
        let shared_ready_rx = Arc::new(Mutex::new(ready_rx));

        let (report_tx, mut report_rx) = mpsc::channel(config.shard_queue_size.max(1));

        let ctx = Arc::new(WorkerContext {
            global_semaphore: Arc::new(Semaphore::new(config.max_in_flight)),
            endpoint_configs: RwLock::new(HashMap::new()),
            endpoint_semaphores: RwLock::new(HashMap::new()),
            report_tx: report_tx.clone(),
            rate_limiters: RwLock::new(HashMap::new()),
            tenant_rate_limiters: RwLock::new(HashMap::new()),
            #[cfg(feature = "http")]
            http_client: reqwest::Client::new(),
        });

        let mut worker_handles = Vec::new();

        for _ in 0..config.worker_count {
            worker_handles.push(tokio::spawn(worker_loop(
                shared_ready_rx.clone(),
                ctx.clone(),
            )));
        }

        let notify = Arc::new(Notify::new());
        let is_running = Arc::new(AtomicBool::new(true));
        let idempotency = Arc::new(RwLock::new(HashSet::new()));
        let dlq = Arc::new(Mutex::new(VecDeque::new()));
        let status = Arc::new(RwLock::new(HashMap::new()));

        // ctx already initialized above with report_tx.
        let mut shard_senders = Vec::with_capacity(shard_count);
        let mut shard_receivers = Vec::with_capacity(shard_count);
        let mut overflow_buffers = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            let (tx, rx) = mpsc::channel(config.shard_queue_size.max(1));
            shard_senders.push(tx);
            shard_receivers.push(Mutex::new(rx));
            overflow_buffers.push(Mutex::new(VecDeque::new()));
        }

        let scheduler_ready_tx = ready_tx.clone();
        let scheduler_notify = notify.clone();
        let scheduler_running = is_running.clone();
        let scheduler_idempotency = idempotency.clone();
        let scheduler_dlq = dlq.clone();
        let scheduler_config = config.clone();
        let scheduler_storage = storage.clone();
        let scheduler_status = status.clone();
        let mut scheduler_shard_senders = shard_senders.clone();
        let scheduler_overflow_buffers = Arc::new(overflow_buffers);
        let scheduler_overflow_buffers_handle = scheduler_overflow_buffers.clone();

        let scheduler_ctx = ctx.clone();
        let scheduler_handle = tokio::spawn(async move {
            let mut next_index = 0usize;
            let mut delay_heap: BinaryHeap<TimedTask> = BinaryHeap::new();

            loop {
                let mut moved_any = false;
                let mut all_disconnected = true;
                let running = scheduler_running.load(Ordering::SeqCst);

                if !running {
                    scheduler_shard_senders.clear();
                }

                // Move due retries into shard queues.
                let now = Instant::now();
                while let Some(task) = delay_heap.peek() {
                    if task.ready_at > now {
                        break;
                    }
                    let task = delay_heap.pop().expect("pop");
                    let shard_index = shard_for_endpoint(&task.task.endpoint_id, shard_receivers.len());
                    if shard_index < shard_receivers.len() {
                        if let Some(tx) = scheduler_shard_senders.get(shard_index) {
                            let _ = tx.send(task.task).await;
                            scheduler_notify.notify_one();
                        }
                    }
                }

                for offset in 0..shard_receivers.len() {
                    let index = (next_index + offset) % shard_receivers.len();
                    if let Some(buffer) = scheduler_overflow_buffers_handle.get(index) {
                        let mut buffer = buffer.lock().await;
                        if let Some(task) = buffer.pop_front() {
                            moved_any = true;
                            all_disconnected = false;
                            if scheduler_ready_tx.send(task).await.is_err() {
                                return;
                            }
                            next_index = (index + 1) % shard_receivers.len();
                            break;
                        }
                    }
                    let mut guard = shard_receivers[index].lock().await;
                    match guard.try_recv() {
                        Ok(task) => {
                            moved_any = true;
                            all_disconnected = false;
                            if scheduler_ready_tx.send(task).await.is_err() {
                                return;
                            }
                            next_index = (index + 1) % shard_receivers.len();
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {}
                    }

                    if !guard.is_closed() {
                        all_disconnected = false;
                    }
                }

                // Handle worker reports (deliveries, retries, DLQ).
                loop {
                    match report_rx.try_recv() {
                        Ok(report) => {
                            handle_report(
                                report,
                                &scheduler_idempotency,
                                &scheduler_dlq,
                                &scheduler_config,
                                &scheduler_storage,
                                &scheduler_status,
                                &scheduler_ctx.endpoint_configs,
                                &mut delay_heap,
                            ).await;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                if !moved_any {
                    if !running && all_disconnected && delay_heap.is_empty() {
                        return;
                    }

                    if let Some(next_ready) = delay_heap.peek().map(|t| t.ready_at) {
                        tokio::select! {
                            _ = scheduler_notify.notified() => {}
                            _ = sleep_until(next_ready) => {}
                        }
                    } else {
                        scheduler_notify.notified().await;
                    }
                }
            }
        });

        Self {
            ready_tx: Some(ready_tx),
            is_running,
            worker_handles,
            scheduler_handle: Some(scheduler_handle),
            ctx,
            shard_senders,
            overflow_buffers: scheduler_overflow_buffers,
            notify,
            idempotency,
            dlq,
            storage,
            status,
            config,
        }
    }

    pub async fn register_endpoint(&self, endpoint: Endpoint) {
        let id = endpoint.id.clone();

        {
            let mut guard = self.ctx.endpoint_semaphores.write().await;
            guard.insert(
                id.clone(),
                Arc::new(Semaphore::new(endpoint.max_concurrent)),
            );
        }

        {
            let mut guard = self.ctx.endpoint_configs.write().await;
            guard.insert(id.clone(), Arc::new(endpoint));
        }

        self.maybe_register_rate_limiter(&id).await;
    }

    async fn maybe_register_rate_limiter(&self, endpoint_id: &EndpointId) {
        let endpoint = {
            let guard = self.ctx.endpoint_configs.read().await;
            guard.get(endpoint_id).cloned()
        };

        let Some(endpoint) = endpoint else { return; };
        let Some(max_rps) = endpoint.max_rps else { return; };
        let burst = endpoint.burst.unwrap_or(max_rps);

        let mut guard = self.ctx.rate_limiters.write().await;
        guard.insert(
            endpoint_id.clone(),
            Arc::new(Mutex::new(TokenBucket::new(burst, max_rps))),
        );
    }

    pub async fn set_tenant_rate_limit(&self, tenant_id: TenantId, max_rps: u32, burst: u32) {
        let mut guard = self.ctx.tenant_rate_limiters.write().await;
        guard.insert(
            tenant_id,
            Arc::new(Mutex::new(TokenBucket::new(burst, max_rps))),
        );
    }

    pub async fn dispatch(
        &self,
        event: Event,
        endpoint_ids: Vec<EndpointId>,
    ) -> Result<(), DispatchError> {
        if !self.is_running.load(Ordering::SeqCst) {
            return Err(DispatchError::Shutdown);
        }

        for endpoint_id in endpoint_ids {
            let endpoint = match self.get_endpoint(&endpoint_id).await {
                Some(e) => e,
                None => {
                    metric_inc("webhook.dispatch.invalid_endpoint");
                    return Err(DispatchError::InvalidEndpoint { endpoint_id });
                }
            };

            if let Some(ref tenant) = endpoint.tenant_id {
                if event.tenant_id.as_ref() != Some(tenant) {
                    metric_inc("webhook.dispatch.invalid_endpoint");
                    return Err(DispatchError::InvalidEndpoint { endpoint_id });
                }
            }

            if let Some(ref tenant) = event.tenant_id {
                self.ensure_tenant_rate_limiter(tenant).await;
            }

            if !self.is_endpoint_registered(&endpoint_id).await {
                metric_inc("webhook.dispatch.invalid_endpoint");
                return Err(DispatchError::InvalidEndpoint { endpoint_id });
            }

            let task = Task {
                event: event.clone(),
                endpoint_id,
                attempt: 0,
            };

            let key = IdempotencyKey::new(
                task.event.id.clone(),
                task.endpoint_id.clone(),
                task.event.tenant_id.clone(),
            );
            if self.is_delivered(&key).await {
                continue;
            }
            self.update_status(
                &key,
                DeliveryStatus::Queued,
                task.attempt,
                None,
            ).await;

            let shard_index = shard_for_endpoint(&task.endpoint_id, self.shard_senders.len());
            let shard_tx = &self.shard_senders[shard_index];

            let task_clone = task.clone();
            match shard_tx.try_send(task) {
                Ok(_) => {
                    self.storage.record_enqueue(&task_clone).await;
                    metric_inc("webhook.dispatch.enqueued");
                    self.notify.notify_one();
                }
                Err(mpsc::error::TrySendError::Full(task)) => {
                    self.handle_overflow(shard_index, task, task_clone).await?;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    metric_inc("webhook.dispatch.shutdown");
                    return Err(DispatchError::Shutdown);
                }
            }
        }

        trace_event("webhook.dispatch.completed");
        Ok(())
    }

    async fn is_endpoint_registered(&self, endpoint_id: &EndpointId) -> bool {
        let guard = self.ctx.endpoint_configs.read().await;
        guard.contains_key(endpoint_id)
    }

    pub async fn shutdown(&mut self) {
        self.is_running.store(false, Ordering::SeqCst);
        self.ready_tx.take();
        self.shard_senders.clear();

        self.notify.notify_waiters();
        if let Some(handle) = self.scheduler_handle.take() {
            let _ = handle.await;
        }

        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    pub async fn dlq_snapshot(&self) -> Vec<DlqEntry> {
        let guard = self.dlq.lock().await;
        guard.iter().cloned().collect()
    }

    pub async fn dlq_snapshot_for_tenant(&self, tenant_id: &TenantId) -> Vec<DlqEntry> {
        let guard = self.dlq.lock().await;
        guard
            .iter()
            .filter(|entry| entry.key.tenant_id.as_ref() == Some(tenant_id))
            .cloned()
            .collect()
    }

    pub async fn delivery_status(&self, key: &IdempotencyKey) -> Option<DeliveryState> {
        let guard = self.status.read().await;
        guard.get(key).cloned()
    }

    pub async fn delivery_status_all(&self) -> HashMap<IdempotencyKey, DeliveryState> {
        let guard = self.status.read().await;
        guard.clone()
    }

    pub async fn delivery_status_for_event(
        &self,
        event_id: &crate::types::EventId,
    ) -> HashMap<EndpointId, DeliveryState> {
        let guard = self.status.read().await;
        guard
            .iter()
            .filter_map(|(key, state)| {
                if &key.event_id == event_id {
                    Some((key.endpoint_id.clone(), state.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn replay_dlq_all(&self) -> usize {
        self.replay_dlq_filtered(|_| true).await
    }

    pub async fn replay_dlq_entry(&self, key: &IdempotencyKey) -> bool {
        let entry = {
            let mut guard = self.dlq.lock().await;
            if let Some(index) = guard.iter().position(|e| &e.key == key) {
                Some(guard.remove(index).expect("remove"))
            } else {
                None
            }
        };

        match entry {
            Some(entry) => {
                self.storage.remove_dlq(&entry.key).await;
                self.replay_dlq_entry_inner(entry).await
            }
            None => false,
        }
    }

    pub async fn replay_dlq_for_endpoint(&self, endpoint_id: &EndpointId) -> usize {
        self.replay_dlq_filtered(|entry| &entry.endpoint_id == endpoint_id).await
    }

    pub async fn replay_dlq_for_time_range(&self, start_secs: u64, end_secs: u64) -> usize {
        self.replay_dlq_filtered(|entry| entry.created_at_secs >= start_secs && entry.created_at_secs <= end_secs).await
    }

    pub async fn replay_dlq_for_tenant(&self, tenant_id: &TenantId) -> usize {
        self.replay_dlq_filtered(|entry| entry.key.tenant_id.as_ref() == Some(tenant_id)).await
    }

    async fn replay_dlq_entry_inner(&self, entry: DlqEntry) -> bool {
        if self.is_delivered(&entry.key).await {
            return false;
        }

        let event = Event {
            id: entry.key.event_id.clone(),
            payload: entry.payload.clone(),
            tenant_id: entry.key.tenant_id.clone(),
        };

        let task = Task {
            event,
            endpoint_id: entry.endpoint_id.clone(),
            attempt: 0,
        };

        let shard_index = shard_for_endpoint(&task.endpoint_id, self.shard_senders.len());
        let shard_tx = &self.shard_senders[shard_index];

        if shard_tx.send(task.clone()).await.is_err() {
            return false;
        }

        self.storage.record_enqueue(&task).await;
        self.update_status(
            &entry.key,
            DeliveryStatus::Queued,
            0,
            None,
        ).await;
        self.notify.notify_one();
        true
    }

    async fn replay_dlq_filtered<F>(&self, mut predicate: F) -> usize
    where
        F: FnMut(&DlqEntry) -> bool,
    {
        let entries = {
            let mut guard = self.dlq.lock().await;
            let mut kept = VecDeque::new();
            let mut selected = Vec::new();

            while let Some(entry) = guard.pop_front() {
                if predicate(&entry) {
                    selected.push(entry);
                } else {
                    kept.push_back(entry);
                }
            }

            *guard = kept;
            selected
        };

        let mut replayed = 0usize;
        for entry in entries {
            self.storage.remove_dlq(&entry.key).await;
            if self.replay_dlq_entry_inner(entry).await {
                replayed += 1;
            }
        }

        replayed
    }

    pub async fn rate_limiter_stats(&self) -> HashMap<EndpointId, RateLimiterStats> {
        let guard = self.ctx.rate_limiters.read().await;
        let mut stats = HashMap::new();
        for (id, bucket) in guard.iter() {
            let bucket = bucket.lock().await;
            stats.insert(id.clone(), bucket.snapshot());
        }
        stats
    }

    pub async fn tenant_rate_limiter_stats(&self) -> HashMap<TenantId, RateLimiterStats> {
        let guard = self.ctx.tenant_rate_limiters.read().await;
        let mut stats = HashMap::new();
        for (id, bucket) in guard.iter() {
            let bucket = bucket.lock().await;
            stats.insert(id.clone(), bucket.snapshot());
        }
        stats
    }

    async fn is_delivered(&self, key: &IdempotencyKey) -> bool {
        let guard = self.idempotency.read().await;
        guard.contains(key)
    }

    async fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<Arc<Endpoint>> {
        let guard = self.ctx.endpoint_configs.read().await;
        guard.get(endpoint_id).cloned()
    }

    async fn ensure_tenant_rate_limiter(&self, tenant_id: &TenantId) {
        let mut guard = self.ctx.tenant_rate_limiters.write().await;
        if guard.contains_key(tenant_id) {
            return;
        }
        if let Some(max_rps) = self.config.tenant_max_rps {
            let burst = self.config.tenant_burst.unwrap_or(max_rps);
            guard.insert(
                tenant_id.clone(),
                Arc::new(Mutex::new(TokenBucket::new(burst, max_rps))),
            );
        }
    }

    async fn update_status(
        &self,
        key: &IdempotencyKey,
        status: DeliveryStatus,
        attempts: u32,
        last_error: Option<String>,
    ) {
        let state = DeliveryState {
            status,
            attempts,
            last_error,
            last_updated_secs: now_secs(),
        };
        let mut guard = self.status.write().await;
        guard.insert(key.clone(), state.clone());
        self.storage.record_status(key, &state).await;
    }

    async fn handle_overflow(
        &self,
        shard_index: usize,
        task: Task,
        task_clone: Task,
    ) -> Result<(), DispatchError> {
        match self.config.overflow_policy {
            OverflowPolicy::DropNewest => {
                metric_inc("webhook.dispatch.backpressure");
                Err(DispatchError::Backpressure)
            }
            OverflowPolicy::Block => {
                let shard_tx = &self.shard_senders[shard_index];
                shard_tx
                    .send(task)
                    .await
                    .map_err(|_| DispatchError::Shutdown)?;
                self.storage.record_enqueue(&task_clone).await;
                metric_inc("webhook.dispatch.enqueued");
                self.notify.notify_one();
                Ok(())
            }
            OverflowPolicy::DropOldest => {
                if let Some(buffer) = self.overflow_buffers.get(shard_index) {
                    let mut buffer = buffer.lock().await;
                    if buffer.len() >= self.config.overflow_buffer_size {
                        buffer.pop_front();
                    }
                    buffer.push_back(task);
                    self.storage.record_enqueue(&task_clone).await;
                    metric_inc("webhook.dispatch.enqueued");
                    self.notify.notify_one();
                    Ok(())
                } else {
                    Err(DispatchError::Backpressure)
                }
            }
            OverflowPolicy::SpillToStorage => {
                self.storage.record_enqueue(&task_clone).await;
                if let Some(buffer) = self.overflow_buffers.get(shard_index) {
                    let mut buffer = buffer.lock().await;
                    if buffer.len() < self.config.overflow_buffer_size {
                        buffer.push_back(task);
                        self.notify.notify_one();
                    }
                }
                metric_inc("webhook.dispatch.enqueued");
                Ok(())
            }
        }
    }
}

fn shard_for_endpoint(endpoint_id: &EndpointId, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    endpoint_id.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count.max(1)
}

#[derive(Debug)]
struct TimedTask {
    ready_at: Instant,
    task: Task,
}

impl Eq for TimedTask {}

impl PartialEq for TimedTask {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at.eq(&other.ready_at)
    }
}

impl Ord for TimedTask {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse for min-heap behavior
        other.ready_at.cmp(&self.ready_at)
    }
}

impl PartialOrd for TimedTask {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

async fn handle_report(
    report: DeliveryReport,
    idempotency: &Arc<RwLock<HashSet<IdempotencyKey>>>,
    dlq: &Arc<Mutex<VecDeque<DlqEntry>>>,
    config: &DispatcherConfig,
    storage: &Arc<dyn Storage>,
    status: &Arc<RwLock<HashMap<IdempotencyKey, DeliveryState>>>,
    endpoint_configs: &RwLock<HashMap<EndpointId, Arc<Endpoint>>>,
    delay_heap: &mut BinaryHeap<TimedTask>,
) {
    match &report.outcome {
        crate::error::DeliveryOutcome::Delivered => {
            let key = IdempotencyKey::new(
                report.task.event.id.clone(),
                report.task.endpoint_id.clone(),
                report.task.event.tenant_id.clone(),
            );
            let mut guard = idempotency.write().await;
            guard.insert(key);
            storage.record_delivery(&report.task, &report.outcome).await;
            update_status_map(status, &report, DeliveryStatus::Delivered, None).await;
            storage
                .record_status(
                    &IdempotencyKey::new(
                        report.task.event.id.clone(),
                        report.task.endpoint_id.clone(),
                        report.task.event.tenant_id.clone(),
                    ),
                    &DeliveryState {
                        status: DeliveryStatus::Delivered,
                        attempts: report.task.attempt,
                        last_error: None,
                        last_updated_secs: now_secs(),
                    },
                )
                .await;
            metric_inc("webhook.delivery.delivered");
        }
        crate::error::DeliveryOutcome::Failed(reason) => {
            if report.retry_after.is_some() {
                let delay = retry_delay_for_attempt(
                    report.task.attempt,
                    config,
                    endpoint_configs,
                    &report.task.endpoint_id,
                ).await;
                let jitter = jitter_delay(endpoint_retry_jitter_ms(
                    config,
                    endpoint_configs,
                    &report.task.endpoint_id,
                ).await);
                let ready_at = Instant::now() + delay + jitter;
                delay_heap.push(TimedTask {
                    ready_at,
                    task: report.task.clone(),
                });
                update_status_map(
                    status,
                    &report,
                    DeliveryStatus::Retrying,
                    Some(format!("{:?}", reason)),
                )
                .await;
                storage
                    .record_status(
                        &IdempotencyKey::new(
                            report.task.event.id.clone(),
                            report.task.endpoint_id.clone(),
                            report.task.event.tenant_id.clone(),
                        ),
                        &DeliveryState {
                            status: DeliveryStatus::Retrying,
                            attempts: report.task.attempt,
                            last_error: Some(format!("{:?}", reason)),
                            last_updated_secs: now_secs(),
                        },
                    )
                    .await;
                metric_inc("webhook.delivery.retry_scheduled");
            } else {
                push_dlq(&report, dlq, config, storage).await;
                storage.record_delivery(&report.task, &crate::error::DeliveryOutcome::Failed(reason.clone())).await;
                update_status_map(
                    status,
                    &report,
                    DeliveryStatus::Dlq,
                    Some(format!("{:?}", reason)),
                )
                .await;
                storage
                    .record_status(
                        &IdempotencyKey::new(
                            report.task.event.id.clone(),
                            report.task.endpoint_id.clone(),
                            report.task.event.tenant_id.clone(),
                        ),
                        &DeliveryState {
                            status: DeliveryStatus::Dlq,
                            attempts: report.task.attempt,
                            last_error: Some(format!("{:?}", reason)),
                            last_updated_secs: now_secs(),
                        },
                    )
                    .await;
                metric_inc("webhook.delivery.failed");
            }
        }
        crate::error::DeliveryOutcome::Dropped(_) => {
            push_dlq(&report, dlq, config, storage).await;
            storage.record_delivery(&report.task, &report.outcome).await;
            update_status_map(
                status,
                &report,
                DeliveryStatus::Dlq,
                Some(format!("{:?}", report.outcome)),
            )
            .await;
            storage
                .record_status(
                    &IdempotencyKey::new(
                        report.task.event.id.clone(),
                        report.task.endpoint_id.clone(),
                        report.task.event.tenant_id.clone(),
                    ),
                    &DeliveryState {
                        status: DeliveryStatus::Dlq,
                        attempts: report.task.attempt,
                        last_error: Some(format!("{:?}", report.outcome)),
                        last_updated_secs: now_secs(),
                    },
                )
                .await;
            metric_inc("webhook.delivery.dropped");
        }
    }
}

async fn push_dlq(
    report: &DeliveryReport,
    dlq: &Arc<Mutex<VecDeque<DlqEntry>>>,
    config: &DispatcherConfig,
    storage: &Arc<dyn Storage>,
) {
    let entry = DlqEntry {
        key: IdempotencyKey::new(
            report.task.event.id.clone(),
            report.task.endpoint_id.clone(),
            report.task.event.tenant_id.clone(),
        ),
        payload: report.task.event.payload.clone(),
        endpoint_id: report.task.endpoint_id.clone(),
        failure: format!("{:?}", report.outcome),
        created_at_secs: now_secs(),
    };

    storage.record_dlq(&entry).await;
    metric_inc("webhook.dlq.inserted");

    let mut guard = dlq.lock().await;
    guard.push_back(entry);
    while guard.len() > config.dlq_capacity {
        guard.pop_front();
    }
}

async fn update_status_map(
    status: &Arc<RwLock<HashMap<IdempotencyKey, DeliveryState>>>,
    report: &DeliveryReport,
    new_status: DeliveryStatus,
    last_error: Option<String>,
) {
    let key = IdempotencyKey::new(
        report.task.event.id.clone(),
        report.task.endpoint_id.clone(),
        report.task.event.tenant_id.clone(),
    );
    let mut guard = status.write().await;
    guard.insert(
        key,
        DeliveryState {
            status: new_status,
            attempts: report.task.attempt,
            last_error,
            last_updated_secs: now_secs(),
        },
    );
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn jitter_delay(jitter_ms: u64) -> Duration {
    if jitter_ms == 0 {
        return Duration::from_millis(0);
    }
    let jitter = fastrand::u64(0..=jitter_ms);
    Duration::from_millis(jitter)
}

async fn retry_delay_for_attempt(
    attempt: u32,
    config: &DispatcherConfig,
    endpoint_configs: &RwLock<HashMap<EndpointId, Arc<Endpoint>>>,
    endpoint_id: &EndpointId,
) -> Duration {
    let (base_ms, max_ms) = {
        let guard = endpoint_configs.read().await;
        if let Some(endpoint) = guard.get(endpoint_id) {
            (
                endpoint.retry_base_ms.unwrap_or(config.retry_base_ms),
                endpoint.retry_max_ms.unwrap_or(config.retry_max_ms),
            )
        } else {
            (config.retry_base_ms, config.retry_max_ms)
        }
    };
    let base = base_ms.max(1);
    let max = max_ms.max(base);
    let pow = 2u64.pow(attempt.saturating_sub(1));
    let exp = base.saturating_mul(pow);
    Duration::from_millis(exp.min(max))
}

async fn endpoint_retry_jitter_ms(
    config: &DispatcherConfig,
    endpoint_configs: &RwLock<HashMap<EndpointId, Arc<Endpoint>>>,
    endpoint_id: &EndpointId,
) -> u64 {
    let guard = endpoint_configs.read().await;
    guard
        .get(endpoint_id)
        .and_then(|e| e.retry_jitter_ms)
        .unwrap_or(config.retry_jitter_ms)
}
