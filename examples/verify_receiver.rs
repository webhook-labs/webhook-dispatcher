use webhook_dispatcher::verify_webhook_request;

fn main() {
    let headers = vec![
        ("X-Webhook-Signature", "abcd..."),
        ("X-Webhook-Timestamp", "1700000000"),
    ];

    let payload = br#"{"id":123}"#;
    let now_secs = 1_700_000_200;

    let _ = verify_webhook_request(
        headers.iter().map(|(k, v)| (*k, *v)),
        payload,
        b"supersecret",
        "X-Webhook-Signature",
        "X-Webhook-Timestamp",
        300,
        now_secs,
    );
}
