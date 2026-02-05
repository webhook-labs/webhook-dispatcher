#[cfg(feature = "postgres")]
use async_trait::async_trait;
#[cfg(feature = "postgres")]
use tokio_postgres::Client;

#[cfg(feature = "postgres")]
use crate::error::DeliveryOutcome;
#[cfg(feature = "postgres")]
use crate::storage::Storage;
#[cfg(feature = "postgres")]
use crate::types::{DlqEntry, DeliveryState, IdempotencyKey, TenantId};
#[cfg(feature = "postgres")]
use crate::worker::Task;

#[cfg(feature = "postgres")]
pub struct PostgresStorage {
    client: Client,
}

#[cfg(feature = "postgres")]
impl PostgresStorage {
    pub async fn new(client: Client) -> Result<Self, tokio_postgres::Error> {
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS webhook_pending (
                    id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL
                )",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS webhook_dlq (
                    id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL
                )",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS webhook_status (
                    id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL
                )",
                &[],
            )
            .await?;

        Ok(Self { client })
    }

    fn task_id(task: &Task) -> String {
        format!("{}|{}", task.event.id.0, task.endpoint_id.0)
    }

    fn key_id(key: &IdempotencyKey) -> String {
        let tenant = key.tenant_id.as_ref().map(|t| t.0.as_str()).unwrap_or("");
        format!("{}|{}|{}", key.event_id.0, key.endpoint_id.0, tenant)
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl Storage for PostgresStorage {
    async fn record_enqueue(&self, task: &Task) {
        let payload = serde_json::to_value(task).unwrap_or_default();
        let _ = self.client.execute(
            "INSERT INTO webhook_pending (id, payload)
             VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload",
            &[&Self::task_id(task), &payload],
        ).await;
    }

    async fn record_delivery(&self, task: &Task, _outcome: &DeliveryOutcome) {
        let _ = self.client.execute(
            "DELETE FROM webhook_pending WHERE id = $1",
            &[&Self::task_id(task)],
        ).await;
    }

    async fn record_dlq(&self, entry: &DlqEntry) {
        let id = Self::key_id(&entry.key);
        let payload = serde_json::to_value(entry).unwrap_or_default();
        let _ = self.client.execute(
            "INSERT INTO webhook_dlq (id, payload)
             VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload",
            &[&id, &payload],
        ).await;
    }

    async fn load_pending(&self) -> Vec<Task> {
        let rows = self.client
            .query("SELECT payload FROM webhook_pending", &[])
            .await
            .unwrap_or_default();

        rows.into_iter()
            .filter_map(|row| row.try_get::<_, serde_json::Value>(0).ok())
            .filter_map(|v| serde_json::from_value::<Task>(v).ok())
            .collect()
    }

    async fn record_status(&self, key: &IdempotencyKey, state: &DeliveryState) {
        let payload = serde_json::to_value(state).unwrap_or_default();
        let _ = self.client.execute(
            "CREATE TABLE IF NOT EXISTS webhook_status (
                id TEXT PRIMARY KEY,
                payload JSONB NOT NULL
            )",
            &[],
        ).await;
        let _ = self.client.execute(
            "INSERT INTO webhook_status (id, payload)
             VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload",
            &[&Self::key_id(key), &payload],
        ).await;
    }

    async fn load_status(&self) -> std::collections::HashMap<IdempotencyKey, DeliveryState> {
        let _ = self.client.execute(
            "CREATE TABLE IF NOT EXISTS webhook_status (
                id TEXT PRIMARY KEY,
                payload JSONB NOT NULL
            )",
            &[],
        ).await;
        let rows = self.client
            .query("SELECT id, payload FROM webhook_status", &[])
            .await
            .unwrap_or_default();

        rows.into_iter()
            .filter_map(|row| {
                let id: String = row.try_get(0).ok()?;
                let payload: serde_json::Value = row.try_get(1).ok()?;
                let mut parts = id.split('|');
                let event = parts.next()?.to_string();
                let endpoint = parts.next()?.to_string();
                let tenant = parts.next().unwrap_or("").to_string();
                let tenant_id = if tenant.is_empty() { None } else { Some(TenantId(tenant)) };
                let state = serde_json::from_value::<DeliveryState>(payload).ok()?;
                Some((
                    IdempotencyKey::new(
                        crate::types::EventId(event),
                        crate::types::EndpointId(endpoint),
                        tenant_id,
                    ),
                    state,
                ))
            })
            .collect()
    }

    async fn remove_dlq(&self, key: &IdempotencyKey) {
        let id = Self::key_id(key);
        let _ = self.client.execute(
            "DELETE FROM webhook_dlq WHERE id = $1",
            &[&id],
        ).await;
    }
}
