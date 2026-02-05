use std::fmt;

use crate::types::EndpointId;

/// Errors returned when dispatching work fails *before* delivery begins.
#[derive(Debug, PartialEq, Eq)]
pub enum DispatchError {
    /// Dispatcher queue is full.
    /// Caller must retry or apply backoff.
    Backpressure,

    /// Dispatcher has been shut down.
    Shutdown,

    /// Endpoint was not registered.
    InvalidEndpoint {
        endpoint_id: EndpointId,
    },
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatchError::Backpressure =>
                write!(f, "dispatcher at capacity"),
            DispatchError::Shutdown =>
                write!(f, "dispatcher is shut down"),
            DispatchError::InvalidEndpoint { endpoint_id } =>
                write!(f, "endpoint not registered: {:?}", endpoint_id),
        }
    }
}

impl std::error::Error for DispatchError {}

/// Final outcome of attempting to deliver a webhook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Dropped(DropReason),
    Failed(FailureReason),
}

/// Reasons why a delivery was dropped *without* an HTTP attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropReason {
    /// Internal capacity exhausted.
    Backpressure,

    /// Dispatcher is shutting down.
    Shutdown,
}

/// Reasons why an HTTP delivery attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReason {
    Timeout,
    Network,
    RemoteError,
    ClientError,
    MaxRetriesExceeded,
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureReason::Timeout =>
                write!(f, "request timed out"),
            FailureReason::Network =>
                write!(f, "network error"),
            FailureReason::RemoteError =>
                write!(f, "remote endpoint returned error"),
            FailureReason::ClientError =>
                write!(f, "client error (non-retryable)"),
            FailureReason::MaxRetriesExceeded =>
                write!(f, "maximum retries exceeded"),
        }
    }
}
