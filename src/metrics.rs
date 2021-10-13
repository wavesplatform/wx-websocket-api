use lazy_static::lazy_static;
use prometheus::{exponential_buckets, Counter, Histogram, HistogramOpts, IntGauge, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref CLIENTS: IntGauge =
        IntGauge::new("Backend_websocket_Clients_count", "Count of clients")
            .expect("can't create clients metrics");
    pub static ref MESSAGES: Counter = Counter::new(
        "Backend_websocket_Messages_count",
        "Count of messages sent to clients"
    )
    .expect("can't create messages metrics");
    pub static ref SUBSCRIBED_MESSAGE_LATENCIES: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "Backend_websocket_Latencies_histogram",
            "Histogram of subscription latency time"
        )
        .buckets(exponential_buckets(0.001, 2.0, 18).expect("can't create histogram buckets"))
    )
    .expect("can't create latencies metrics");
    pub static ref REDIS_INPUT_QUEUE_SIZE: IntGauge = IntGauge::new(
        "Backend_websocket_Queue_size",
        "Size of incoming Redis messages queue"
    )
    .expect("can't create message_queue metrics");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(CLIENTS.clone()))
        .expect("can't register clients metrics");

    REGISTRY
        .register(Box::new(MESSAGES.clone()))
        .expect("can't register messages metrics");

    REGISTRY
        .register(Box::new(SUBSCRIBED_MESSAGE_LATENCIES.clone()))
        .expect("can't register subscribed_message_latencies metrics");

    REGISTRY
        .register(Box::new(REDIS_INPUT_QUEUE_SIZE.clone()))
        .expect("can't register redis_input_queue_size metrics");
}
