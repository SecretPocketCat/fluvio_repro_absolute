use std::time::Duration;

use async_std::stream::StreamExt;
use fluvio::{
    metadata::topic::{CleanupPolicy, SegmentBasedPolicy, TopicSpec, TopicStorageConfig},
    Fluvio, Offset, RecordKey,
};
use tracing::debug;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let fluvio = Fluvio::connect().await?;
    let fluvio_admin = fluvio.admin().await;

    let mut spec = TopicSpec::new_computed(1, 1, None);
    spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
        time_in_seconds: 10,
    }));
    spec.set_storage(TopicStorageConfig {
        segment_size: Some(10000),
        ..Default::default()
    });

    let topic_name = Uuid::new_v4().to_string();
    fluvio_admin.create(topic_name.clone(), false, spec).await?;

    let producer = fluvio.topic_producer(topic_name.clone()).await?;
    producer
        .send_all((0..10000).map(|i| (RecordKey::NULL, i.to_string())))
        .await?;
    producer.flush().await?;

    tokio::time::sleep(Duration::from_secs(11)).await;

    let consumer = fluvio.partition_consumer(topic_name.clone(), 0).await?;
    let mut stream = consumer.stream(Offset::absolute(0)?).await?;

    while let Some(rec) = stream.next().await {
        match rec {
            Ok(rec) => {
                debug!("{}", String::from_utf8_lossy(rec.get_value()));
            }
            Err(e) => anyhow::bail!("Record error: {e}"),
        }
    }

    Ok(())
}
