use std::{fmt::Debug, num::NonZeroUsize};

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
        match self.aggregator.clone() {
            Some(aggregator) => {
                // Aggregated pipeline
                self.run_aggregated_pipeline(input, aggregator).await
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
        aggregator: KplAggregator,
    ) -> Result<(), ()> {
        let Self { base_sink, .. } = *self;
        let KinesisSink {
            batch_settings,
            service,
            request_builder,
            partition_key_field,
            _phantom,
        } = base_sink;

        input
            .filter_map(move |event| {
                let log = event.into_log();
                let processed = process_log(log, partition_key_field.as_ref());
                future::ready(processed)
            })
            .request_builder(
                NonZeroUsize::new(50).unwrap(),
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
            .batched(batch_settings.as_byte_size_config())
            .map(move |kinesis_requests: Vec<KinesisRequest<KinesisStreamRecord>>| {
                // Convert KinesisRequests to UserRecords for aggregation
                let user_records: Vec<UserRecord> = kinesis_requests
                    .into_iter()
                    .map(|mut req| {
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
                    .collect();

                // Apply aggregation
                let aggregated_records = aggregator.aggregate_records(user_records);

                // Convert aggregated records back to KinesisRequests
                let events = aggregated_records
                    .into_iter()
                    .map(|agg_record| {
                        let kinesis_record = KinesisStreamRecord::from_aggregated(&agg_record);
                        KinesisRequest{
                            key: KinesisKey {
                                partition_key: agg_record.partition_key.clone(),
                            },
                            record: kinesis_record,
                            finalizers: agg_record.finalizers,
                            metadata: agg_record.metadata,
                        }
                    })
                    .collect::<Vec<_>>();

                let metadata = RequestMetadata::from_batch(
                    events.iter().map(|req| req.get_metadata().clone())
                );

                BatchKinesisRequest { events, metadata }
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
