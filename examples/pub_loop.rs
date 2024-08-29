use paho_mqtt::{self as mqtt, AsyncClient};
use tokio::time::sleep;

use std::{
    sync::atomic::AtomicU64,
    time::{Duration, Instant},
};

use std::sync::atomic::Ordering;

static COUNT: AtomicU64 = AtomicU64::new(0);

const TOPIC: &str = "/bal/thr";
const QOS: i32 = mqtt::QOS_0;

pub fn cb(_cli: &AsyncClient, _val: u16) {
    COUNT.fetch_add(1, Ordering::Relaxed);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    env_logger::init();

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri("localhost:1883")
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .finalize();
    let connect_opts = mqtt::ConnectOptionsBuilder::new().finalize();

    let client = mqtt::AsyncClient::new(create_opts).unwrap();
    let msg_payload: Vec<_> = (0..8).map(|_| 0u8).collect();

    let _ = client.connect(connect_opts).await;
    let msg = mqtt::Message::new(TOPIC, msg_payload.clone(), QOS);
    let mut local_count = 0;
    let msg_cnt = 1_000_000;

    let now = Instant::now();
    let _ = tokio::spawn(async move {
        loop {
            let _ = client.publish_cb(msg.clone(), cb);

            /*If running as root I have to wait a bit not to cause strange TCP retransmissions

             local_count += 1;

            if local_count == 10_000 {
                sleep(Duration::from_millis(10)).await;
                local_count = 0;
            } */

            if COUNT.fetch_add(0, Ordering::Relaxed) > msg_cnt {
                break;
            }
        }
    })
    .await;
    let elapsed = now.elapsed().as_micros();
    println!(
        "Time to handle {} message: : {:?} seconds",
        msg_cnt,
        (elapsed as f64 / 1_000_000.0)
    );
}
