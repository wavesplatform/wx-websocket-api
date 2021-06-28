use lazy_static::lazy_static;
use prometheus::{Counter, IntGauge, Registry};
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref CLIENTS: IntGauge =
        IntGauge::new("Backend websocket Clients count", "Count of clients")
            .expect("can't create clients metrics");
    pub static ref MESSAGES: Counter = Counter::new(
        "Backend websocket Messages count",
        "Count of messages sended to clients"
    )
    .expect("can't create messages metrics");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(CLIENTS.clone()))
        .expect("can't register clients metrics");

    REGISTRY
        .register(Box::new(MESSAGES.clone()))
        .expect("can't register messages metrics");
}
