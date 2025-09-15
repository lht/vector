use std::{fmt::Debug, num::NonZeroUsize, time::Duration, pin::Pin, task::{Context, Poll}};

use futures::{StreamExt, Stream};
use tokio::time::{interval, Interval};
use super::{aggregation::{KplAggregator, UserRecord}, record::KinesisStreamRecord};
use super::super::sink::{KinesisSink, BatchKinesisRequest, KinesisKey, process_log};
use super::super::request_builder::KinesisRequest;
use crate::{
    internal_events::SinkRequestBuildError,
    sinks::prelude::*,
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
            batch_settings: _,
            service,
            request_builder,
            partition_key_field,
            _phantom,
        } = base_sink;

        futures::StreamExt::filter_map(input, move |event| {
                let log = event.into_log();
                future::ready(process_log(log, partition_key_field.as_ref()))
            })
            .request_builder(
                default_request_builder_concurrency_limit(),
                request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .map(move |kinesis_request: KinesisRequest<KinesisStreamRecord>| {
                // Convert each KinesisRequest to UserRecord for streaming aggregation
                let mut req = kinesis_request;
                let partition_key = req.key.partition_key.clone();
                let data = req.record.record.data.as_ref().to_vec().into();
                let metadata = req.get_metadata().clone();
                let finalizers = req.take_finalizers();

                UserRecord {
                    data,
                    partition_key,
                    explicit_hash_key: None,
                    finalizers,
                    metadata,
                }
            })
            // Use aggregation size for chunk size to maximize aggregation efficiency
            // This allows chunks to fill aggregates to their configured capacity
            .ready_chunks(max_records_per_aggregate)
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
            .batched(
                BatcherSettings::new(
                    Duration::from_secs(1),
                    NonZeroUsize::new(5_000_000).unwrap(), // 5MB AWS limit
                    NonZeroUsize::new(500).unwrap()        // AWS Kinesis PutRecords limit
                ).as_byte_size_config()
            )
            .map(|aggregated_kinesis_requests: Vec<KinesisRequest<KinesisStreamRecord>>| {
                let metadata = RequestMetadata::from_batch(
                    aggregated_kinesis_requests.iter().map(|req| req.get_metadata().clone())
                );

                BatchKinesisRequest {
                    events: aggregated_kinesis_requests,
                    metadata
                }
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
