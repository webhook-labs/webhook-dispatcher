use std::time::Duration;
use webhook_dispatcher::{Dispatcher, DispatcherConfig, Endpoint, Event};

#[tokio::test]
async fn test_endpoint_isolation() {
    // Create dispatcher with small limits
    let config = DispatcherConfig {
        max_in_flight: 2,
        shard_queue_size: 10,
        shard_count: 4,
        worker_count: 2,
        ..Default::default()
    };
    
    let dispatcher = Dispatcher::new(config);
    
    // Register two endpoints with small limits
    let endpoint1 = Endpoint::new("slow", "http://slow.example.com")
        .with_max_concurrent(1);
    
    let endpoint2 = Endpoint::new("fast", "http://fast.example.com")
        .with_max_concurrent(1);
    
    dispatcher.register_endpoint(endpoint1.clone()).await;
    dispatcher.register_endpoint(endpoint2.clone()).await;
    
    // Send events to both endpoints
    let event1 = Event::new("event1", "payload1");
    let event2 = Event::new("event2", "payload2");
    
    // Both should be accepted
    assert!(dispatcher.dispatch(
        event1,
        vec![endpoint1.id.clone()]
    ).await.is_ok());
    
    assert!(dispatcher.dispatch(
        event2,
        vec![endpoint2.id.clone()]
    ).await.is_ok());
    
    // Give workers time to process
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Shutdown
    let mut dispatcher = dispatcher;
    dispatcher.shutdown().await;
}
