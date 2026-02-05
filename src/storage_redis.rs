#[cfg(feature = "redis")]
use async_trait::async_trait;
#[cfg(feature = "redis")]
use redis::AsyncCommands;

#[cfg(feature = "redis")]
use crate::error::DeliveryOutcome;
#[cfg(feature = "redis")]
use crate::storage::Storage;
#[cfg(feature = "redis")]
use crate::types::{DlqEntry, DeliveryState, IdempotencyKey, EventId, EndpointId, TenantId};
#[cfg(feature = "redis")]
use crate::worker::Task;

#[cfg(feature = "redis")]
pub struct RedisStorage {
    client: redis::Client,
    prefix: String,
}

#[cfg(feature = "redis")]
impl RedisStorage {
    pub fn new(client: redis::Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
        }
    }

    fn pending_key(&self) -> String {
        format!("{}:pending", self.prefix)
    }

    fn dlq_key(&self) -> String {
        format!("{}:dlq", self.prefix)
    }

    fn status_key(&self) -> String {
        format!("{}:status", self.prefix)
    }

    fn id_key(key: &IdempotencyKey) -> String {
        let tenant = key.tenant_id.as_ref().map(|t| t.0.as_str()).unwrap_or("");
        format!("{}|{}|{}", key.event_id.0, key.endpoint_id.0, tenant)
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl Storage for RedisStorage {
    async fn record_enqueue(&self, task: &Task) {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let _ = conn
            .rpush(self.pending_key(), serde_json::to_string(task).unwrap_or_default())
            .await;
    }

    async fn record_delivery(&self, task: &Task, _outcome: &DeliveryOutcome) {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let payload = serde_json::to_string(task).unwrap_or_default();
        let _ = conn.lrem(self.pending_key(), 1, payload).await;
    }

    async fn record_dlq(&self, entry: &DlqEntry) {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let _ = conn
            .rpush(self.dlq_key(), serde_json::to_string(entry).unwrap_or_default())
            .await;
    }

    async fn load_pending(&self) -> Vec<Task> {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let values: Vec<String> = conn.lrange(self.pending_key(), 0, -1).await.unwrap_or_default();
        values
            .into_iter()
            .filter_map(|v| serde_json::from_str::<Task>(&v).ok())
            .collect()
    }

    async fn record_status(&self, key: &IdempotencyKey, state: &DeliveryState) {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let payload = serde_json::to_string(state).unwrap_or_default();
        let _ = conn.hset(self.status_key(), Self::id_key(key), payload).await;
    }

    async fn load_status(&self) -> std::collections::HashMap<IdempotencyKey, DeliveryState> {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return std::collections::HashMap::new(),
        };
        let map: std::collections::HashMap<String, String> =
            conn.hgetall(self.status_key()).await.unwrap_or_default();
        map.into_iter()
            .filter_map(|(id, value)| {
                let mut parts = id.split('|');
                let event = parts.next()?.to_string();
                let endpoint = parts.next()?.to_string();
                let tenant = parts.next().unwrap_or("").to_string();
                let tenant_id = if tenant.is_empty() { None } else { Some(TenantId(tenant)) };
                let state = serde_json::from_str::<DeliveryState>(&value).ok()?;
                Some((IdempotencyKey::new(EventId(event), EndpointId(endpoint), tenant_id), state))
            })
            .collect()
    }

    async fn remove_dlq(&self, key: &IdempotencyKey) {
        let mut conn = match self.client.get_tokio_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let payloads: Vec<String> = conn.lrange(self.dlq_key(), 0, -1).await.unwrap_or_default();
        for payload in payloads {
            if let Ok(entry) = serde_json::from_str::<DlqEntry>(&payload) {
                if &entry.key == key {
                    let _ = conn.lrem(self.dlq_key(), 1, payload).await;
                    break;
                }
            }
        }
    }
}
