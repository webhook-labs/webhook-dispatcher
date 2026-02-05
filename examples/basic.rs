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

    let _ = dispatcher
        .dispatch(event, vec![webhook_dispatcher::EndpointId("orders".to_string())])
        .await;
}
