use std::{borrow::Cow, fmt::Debug, marker::PhantomData};

use rand::random;
use vector_lib::lookup::lookup_v2::ConfigValuePath;
use vrl::path::PathPrefix;

use super::{
    record::Record,
    request_builder::{KinesisRequest, KinesisRequestBuilder, KinesisAggregationRequestBuilder, KinesisUserRequest},
    aggregation::KplAggregator,
};
use crate::{
    internal_events::{AwsKinesisStreamNoPartitionKeyError, SinkRequestBuildError},
    sinks::{
        prelude::*,
        util::{StreamSink, processed_event::ProcessedEvent},
    },
};

pub type KinesisProcessedEvent = ProcessedEvent<LogEvent, KinesisKey>;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct KinesisKey {
    pub partition_key: String,
}

#[derive(Clone)]
pub struct KinesisSink<S, R> {
    pub batch_settings: BatcherSettings,
    pub service: S,
    pub request_builder: KinesisRequestBuilder<R>,
    pub aggregation_request_builder: Option<KinesisAggregationRequestBuilder<R>>,
    pub partition_key_field: Option<ConfigValuePath>,
    pub aggregator: Option<KplAggregator>,
    pub _phantom: PhantomData<R>,
}

impl<S, R> KinesisSink<S, R>
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
                // Current non-aggregated pipeline (unchanged)
                self.run_standard_pipeline(input).await
            }
        }
    }
    
    async fn run_aggregated_pipeline(
        self: Box<Self>,
        input: BoxStream<'_, Event>,
        aggregator: KplAggregator,
    ) -> Result<(), ()> {
        let batch_settings = self.batch_settings;
        let aggregation_request_builder = self.aggregation_request_builder
            .expect("aggregation_request_builder must be set for aggregated pipeline");
        let service = self.service;
        let partition_key_field = self.partition_key_field;
        
        input
            .filter_map(move |event| {
                let log = event.into_log();
                let processed = process_log(log, partition_key_field.as_ref());
                future::ready(processed)
            })
            .request_builder(
                default_request_builder_concurrency_limit(),
                aggregation_request_builder,
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
    
    async fn run_standard_pipeline(
        self: Box<Self>,
        input: BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        // Current implementation unchanged
        let batch_settings = self.batch_settings;
        let request_builder = self.request_builder;
        let service = self.service;
        let partition_key_field = self.partition_key_field;
        
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
                    Ok(req) => Some(req),
                }
            })
            .batched(batch_settings.as_byte_size_config())
            .map(|events| {
                let metadata = RequestMetadata::from_batch(
                    events.iter().map(|req| req.get_metadata().clone()),
                );
                BatchKinesisRequest { events, metadata }
            })
            .into_driver(service)
            .run()
            .await
    }
}

#[async_trait]
impl<S, R> StreamSink<Event> for KinesisSink<S, R>
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

/// Returns a `KinesisProcessedEvent` containing the unmodified log event + metadata consisting of
/// the partition key. The partition key is either generated from the provided partition_key_field
/// or is generated randomly.
///
/// If the provided partition_key_field was not found in the log, `Error` `EventsDropped` internal
/// events are emitted and None is returned.
pub(crate) fn process_log(
    log: LogEvent,
    partition_key_field: Option<&ConfigValuePath>,
) -> Option<KinesisProcessedEvent> {
    let partition_key = if let Some(partition_key_field) = partition_key_field {
        if let Some(v) = log.get((PathPrefix::Event, partition_key_field)) {
            v.to_string_lossy()
        } else {
            emit!(AwsKinesisStreamNoPartitionKeyError {
                partition_key_field: partition_key_field.0.to_string().as_str()
            });
            return None;
        }
    } else {
        Cow::Owned(gen_partition_key())
    };
    let partition_key = if partition_key.len() >= 256 {
        partition_key[..256].to_string()
    } else {
        partition_key.into_owned()
    };

    Some(KinesisProcessedEvent {
        event: log,
        metadata: KinesisKey { partition_key },
    })
}

fn gen_partition_key() -> String {
    random::<[char; 16]>()
        .iter()
        .fold(String::new(), |mut s, c| {
            s.push(*c);
            s
        })
}

pub struct BatchKinesisRequest<R>
where
    R: Record + Clone,
{
    pub events: Vec<KinesisRequest<R>>,
    pub metadata: RequestMetadata,
}

impl<R> Clone for BatchKinesisRequest<R>
where
    R: Record + Clone,
{
    fn clone(&self) -> Self {
        Self {
            events: self.events.to_vec(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<R> Finalizable for BatchKinesisRequest<R>
where
    R: Record + Clone,
{
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.events.take_finalizers()
    }
}

impl<R> MetaDescriptive for BatchKinesisRequest<R>
where
    R: Record + Clone,
{
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}
