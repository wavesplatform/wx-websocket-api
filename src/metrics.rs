use lazy_static::lazy_static;
use prometheus::{exponential_buckets, Counter, Histogram, HistogramOpts, IntGauge};

lazy_static! {
    pub static ref CLIENTS: IntGauge =
        IntGauge::new("Backend_websocket_Clients_count", "Count of clients")
            .expect("can't create clients metrics");

    pub static ref CLIENT_CONNECT: Counter = Counter::new(
        "Backend_websocket_Client_Connected",
        "Client connect events"
    )
    .expect("can't create Client_Connected metrics");

    pub static ref CLIENT_DISCONNECT: Counter = Counter::new(
        "Backend_websocket_Client_Disconnected",
        "Client disconnect events"
    )
    .expect("can't create Client_Disconnected metrics");

    pub static ref TOPICS: IntGauge = IntGauge::new(
        "Backend_websocket_Topics_count",
        "Count of topics subscribed to"
    )
    .expect("can't create topics metrics");

    pub static ref TOPIC_SUBSCRIBED: Counter = Counter::new(
        "Backend_websocket_Topic_Subscribed",
        "Topic subscribed events"
    )
    .expect("can't create Topic_Subscribed metrics");

    pub static ref TOPIC_UNSUBSCRIBED: Counter = Counter::new(
        "Backend_websocket_Topic_Unsubscribed",
        "Topic unsubscribed events"
    )
    .expect("can't create Topic_Unsubscribed metrics");

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

    pub static ref TOPICS_HASHMAP_SIZE: IntGauge = IntGauge::new(
        "Backend_websocket_Topics_hashmap_size",
        "Size of the global 'Topics' hashmap"
    )
    .expect("can't create TOPICS_HASHMAP_SIZE metrics");

    pub static ref TOPICS_HASHMAP_CAPACITY: IntGauge = IntGauge::new(
        "Backend_websocket_Topics_hashmap_capacity",
        "Capacity of the global 'Topics' hashmap"
    )
    .expect("can't create TOPICS_HASHMAP_CAPACITY metrics");

    // pub static ref CLIENTS_HASHMAP_SIZE: IntGauge = IntGauge::new(
    //     "Backend_websocket_Clients_hashmap_size",
    //     "Size of the global 'Clients' hashmap"
    // )
    // .expect("can't create CLIENTS_HASHMAP_SIZE metrics");
    // pub static ref CLIENTS_HASHMAP_CAPACITY: IntGauge = IntGauge::new(
    //     "Backend_websocket_Clients_hashmap_capacity",
    //     "Capacity of the global 'Clients' hashmap"
    // )
    // .expect("can't create CLIENTS_HASHMAP_CAPACITY metrics");
}
