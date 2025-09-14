use std::fmt::Debug;

use super::aggregation::KplAggregator;
use super::super::{
    record::Record,
    request_builder::{KinesisRequest, KinesisBuilderOutput, KinesisUserRequest},
    sink::{KinesisSink, BatchKinesisRequest, KinesisKey, process_log},
};
use crate::{
    internal_events::SinkRequestBuildError,
    sinks::{
        prelude::*,
        util::{StreamSink, request_builder::default_request_builder_concurrency_limit},
    },
};

/// Kinesis Streams-specific sink that supports KPL aggregation
#[derive(Clone)]
pub struct StreamsKinesisSink<S, R> {
    pub base_sink: KinesisSink<S, R>,
    pub aggregator: Option<KplAggregator>,
}

impl<S, R> StreamsKinesisSink<S, R>
where
    S: Service<BatchKinesisRequest<R>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: Record + Send + Sync + Unpin + Clone + 'static,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        match self.aggregator.clone() {
            Some(aggregator) => {
                // Aggregated pipeline
                self.run_aggregated_pipeline(input, aggregator).await
            }
            None => {
                // Standard pipeline - use base sink logic directly
                Box::new(self.base_sink).run(input).await
            }
        }
    }
    
    async fn run_aggregated_pipeline(
        self: Box<Self>,
        input: BoxStream<'_, Event>,
        aggregator: KplAggregator,
    ) -> Result<(), ()> {
        let batch_settings = self.base_sink.batch_settings;
        let request_builder = self.base_sink.request_builder;
        let service = self.base_sink.service;
        let partition_key_field = self.base_sink.partition_key_field;

        input
            .filter_map(move |event| {
                let log = event.into_log();
                let processed = process_log(log, partition_key_field.as_ref());
                future::ready(processed)
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
                    Ok(KinesisBuilderOutput::Aggregation(req)) => Some(req),
                    Ok(KinesisBuilderOutput::Standard(_)) => {
                        // This shouldn't happen in aggregation mode
                        None
                    }
                }
            })
            .batched(batch_settings.as_byte_size_config())
            .map(move |user_requests: Vec<KinesisUserRequest>| {
                // Extract user records from requests
                let user_records: Vec<_> = user_requests.into_iter()
                    .map(|req| req.user_record)
                    .collect();
                // Apply aggregation
                let aggregated_records = aggregator.aggregate_records(user_records);

                // Convert to batch request
                let events = aggregated_records
                    .into_iter()
                    .map(|agg_record| {
                        let kinesis_record = R::from_aggregated(&agg_record);
                        KinesisRequest::new(
                            KinesisKey {
                                partition_key: agg_record.partition_key.clone(),
                            },
                            kinesis_record,
                            agg_record.finalizers,
                            agg_record.metadata,
                        )
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
impl<S, R> StreamSink<Event> for StreamsKinesisSink<S, R>
where
    S: Service<BatchKinesisRequest<R>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: Record + Send + Sync + Unpin + Clone + 'static,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}