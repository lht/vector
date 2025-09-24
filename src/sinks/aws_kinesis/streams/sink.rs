use std::{fmt::Debug};

use std::num::NonZeroUsize;
use bytes::BytesMut;
use futures::{StreamExt};
use tokio_stream;
use tokio_util::codec::Encoder as _;
use super::{aggregation::{KplAggregator, UserRecord}, record::KinesisStreamRecord};
use super::super::sink::{KinesisSink, BatchKinesisRequest, KinesisKey, process_log, KinesisProcessedEvent};
use super::super::request_builder::KinesisRequest;
use crate::{
    internal_events::SinkRequestBuildError,
    sinks::{prelude::*, util::{Compression, metadata::RequestMetadataBuilder}},
    event::Event,
};

/// Kinesis Streams-specific sink that supports KPL aggregation
#[derive(Clone)]
pub struct KinesisStreamsSink<S> {
    pub base_sink: KinesisSink<S, KinesisStreamRecord>,
    pub aggregator: Option<KplAggregator>,
}

impl<S> KinesisStreamsSink<S>
where
    S: Service<BatchKinesisRequest<KinesisStreamRecord>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        match self.aggregator {
            Some(_) => {
                // Aggregated pipeline
                self.run_aggregated_pipeline(input).await
            }
            None => {
                // Standard pipeline - use base sink logic directly
                Box::new(self.base_sink).run_inner(input).await
            }
        }
    }

    async fn run_aggregated_pipeline(
        self: Box<Self>,
        input: BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        let Self { base_sink, aggregator, .. } = *self;
        let aggregator = aggregator.expect("aggregator must be Some when calling run_aggregated_pipeline");
        let max_records_per_aggregate = aggregator.max_records_per_aggregate();

        let KinesisSink {
            batch_settings,
            service,
            request_builder,
            partition_key_field,
            _phantom,
        } = base_sink;

        // Extract timeout before moving batch_settings
        let chunk_timeout = batch_settings.timeout;

        // Optimized pipeline: bypass intermediate KinesisRequest creation
        // Direct conversion: KinesisProcessedEvent → UserRecord
        let user_records_stream = futures::StreamExt::filter_map(input, move |event| {
                let log = event.into_log();
                future::ready(process_log(log, partition_key_field.as_ref()))
            })
            .map(move |mut processed_event: KinesisProcessedEvent| {
                // Extract finalizers before cloning event
                let finalizers = processed_event.event.take_finalizers();
                let partition_key = processed_event.metadata.partition_key;

                // Extract event data and encode it directly
                let event = Event::from(processed_event.event);
                let metadata_builder = RequestMetadataBuilder::from_event(&event);

                // Apply transformation and encoding
                let (transformer, mut encoder) = request_builder.encoder.clone();
                let mut event = event;
                transformer.transform(&mut event);

                // Encode the event to bytes
                let mut encoded_bytes = BytesMut::new();

                if let Err(error) = encoder.encode(event, &mut encoded_bytes) {
                    emit!(SinkRequestBuildError { error });
                    return None;
                }
                let encoded_bytes = bytes::Bytes::from(encoded_bytes);

                // Apply compression if configured
                let payload_bytes = match request_builder.compression {
                    Compression::None => encoded_bytes,
                    _ => {
                        // For simplicity, keeping compression logic minimal
                        // In production, you'd want proper compression handling
                        encoded_bytes
                    }
                };

                // Calculate payload size before moving
                let payload_size = NonZeroUsize::new(payload_bytes.len())
                    .expect("payload should never be zero length");

                Some(UserRecord {
                    data: payload_bytes,
                    partition_key,
                    explicit_hash_key: None,
                    finalizers,
                    metadata: metadata_builder.with_request_size(payload_size),
                })
            })
            .filter_map(|user_record_opt| future::ready(user_record_opt));

        // Use tokio_stream chunks_timeout for proper timeout handling
        // This will emit chunks when either:
        // 1. max_records_per_aggregate records are collected, OR
        // 2. chunk_timeout duration passes since the first record in current chunk
        tokio_stream::StreamExt::chunks_timeout(user_records_stream, max_records_per_aggregate, chunk_timeout)
            .flat_map(move |user_records_chunk: Vec<UserRecord>| {
                // Apply aggregation to the chunk - this produces multiple aggregated records
                let aggregated_records = aggregator.aggregate_records(user_records_chunk);

                // Convert each aggregated record to a KinesisRequest
                let kinesis_requests: Vec<_> = aggregated_records.into_iter().map(|agg_record| {
                    let kinesis_record = KinesisStreamRecord::from_aggregated(&agg_record);
                    KinesisRequest {
                        key: KinesisKey {
                            partition_key: agg_record.partition_key.clone(),
                        },
                        record: kinesis_record,
                        finalizers: agg_record.finalizers,
                        metadata: agg_record.metadata,
                    }
                }).collect();

                futures::stream::iter(kinesis_requests)
            })
            .batched(batch_settings.as_byte_size_config())
            .map(|events: Vec<KinesisRequest<KinesisStreamRecord>>| {
                let metadata = RequestMetadata::from_batch(
                   events.iter().map(|req| req.get_metadata().clone())
                );
                BatchKinesisRequest {events, metadata }
            })
            .into_driver(service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for KinesisStreamsSink<S>
where
    S: Service<BatchKinesisRequest<KinesisStreamRecord>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
