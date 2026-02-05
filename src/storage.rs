use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::error::DeliveryOutcome;
use crate::types::{DlqEntry, IdempotencyKey, DeliveryState};
use crate::worker::Task;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn record_enqueue(&self, task: &Task);
    async fn record_delivery(&self, task: &Task, outcome: &DeliveryOutcome);
    async fn record_dlq(&self, entry: &DlqEntry);
    async fn load_pending(&self) -> Vec<Task>;
    async fn record_status(&self, key: &IdempotencyKey, state: &DeliveryState);
    async fn load_status(&self) -> HashMap<IdempotencyKey, DeliveryState>;
    async fn remove_dlq(&self, key: &IdempotencyKey);
}

/// In-memory storage for lightweight deployments.
#[derive(Default)]
pub struct InMemoryStorage {
    pending: Mutex<Vec<Task>>,
    dlq: Mutex<Vec<DlqEntry>>,
    delivered: Mutex<Vec<IdempotencyKey>>,
    status: Mutex<HashMap<IdempotencyKey, DeliveryState>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn record_enqueue(&self, task: &Task) {
        self.pending.lock().await.push(task.clone());
    }

    async fn record_delivery(&self, task: &Task, outcome: &DeliveryOutcome) {
        let mut pending = self.pending.lock().await;
        pending.retain(|t| t.event.id != task.event.id || t.endpoint_id != task.endpoint_id);

        if matches!(outcome, DeliveryOutcome::Delivered) {
            let key = IdempotencyKey::new(
                task.event.id.clone(),
                task.endpoint_id.clone(),
                task.event.tenant_id.clone(),
            );
            self.delivered.lock().await.push(key);
        }
    }

    async fn record_dlq(&self, entry: &DlqEntry) {
        self.dlq.lock().await.push(entry.clone());
    }

    async fn load_pending(&self) -> Vec<Task> {
        self.pending.lock().await.clone()
    }

    async fn record_status(&self, key: &IdempotencyKey, state: &DeliveryState) {
        self.status.lock().await.insert(key.clone(), state.clone());
    }

    async fn load_status(&self) -> HashMap<IdempotencyKey, DeliveryState> {
        self.status.lock().await.clone()
    }

    async fn remove_dlq(&self, key: &IdempotencyKey) {
        let mut dlq = self.dlq.lock().await;
        dlq.retain(|entry| &entry.key != key);
    }
}
