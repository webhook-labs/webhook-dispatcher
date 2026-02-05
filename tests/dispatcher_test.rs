use std::time::Duration;
use webhook_dispatcher::{
    Dispatcher,
    DispatcherConfig,
    Endpoint,
    Event,
    EndpointId,
    IdempotencyKey,
    TenantId,
};

#[tokio::test]
async fn test_endpoint_isolation() {
    let config = DispatcherConfig {
        max_in_flight: 2,
        shard_queue_size: 10,
        shard_count: 4,
        worker_count: 2,
        ..Default::default()
    };
    
    let dispatcher = Dispatcher::new(config);
    
    let endpoint1 = Endpoint::new("slow", "http://slow.example.com")
        .with_max_concurrent(1);
    
    let endpoint2 = Endpoint::new("fast", "http://fast.example.com")
        .with_max_concurrent(1);
    
    dispatcher.register_endpoint(endpoint1.clone()).await;
    dispatcher.register_endpoint(endpoint2.clone()).await;
    
    let event1 = Event::new("event1", "payload1");
    let event2 = Event::new("event2", "payload2");
    
    assert!(dispatcher.dispatch(
        event1,
        vec![endpoint1.id.clone()]
    ).await.is_ok());
    
    assert!(dispatcher.dispatch(
        event2,
        vec![endpoint2.id.clone()]
    ).await.is_ok());
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let mut dispatcher = dispatcher;
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn test_invalid_endpoint_rejected() {
    let dispatcher = Dispatcher::new(DispatcherConfig::default());
    let event = Event::new("evt_invalid", "payload");

    let result = dispatcher
        .dispatch(event, vec![EndpointId("missing".to_string())])
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_tenant_mismatch_rejected() {
    let dispatcher = Dispatcher::new(DispatcherConfig::default());

    let endpoint = Endpoint::new("orders", "http://example.com")
        .with_tenant_id("tenant_a");
    dispatcher.register_endpoint(endpoint).await;

    let event = Event::new("evt_tenant", "payload")
        .with_tenant_id("tenant_b");

    let result = dispatcher
        .dispatch(event, vec![EndpointId("orders".to_string())])
        .await;

    assert!(result.is_err());
}

#[test]
fn test_idempotency_key_includes_tenant() {
    let event_id = webhook_dispatcher::EventId("evt".to_string());
    let endpoint_id = EndpointId("orders".to_string());

    let key_no_tenant = IdempotencyKey::new(event_id.clone(), endpoint_id.clone(), None);
    let key_with_tenant = IdempotencyKey::new(
        event_id,
        endpoint_id,
        Some(TenantId("tenant_a".to_string())),
    );

    assert_ne!(key_no_tenant, key_with_tenant);
}
