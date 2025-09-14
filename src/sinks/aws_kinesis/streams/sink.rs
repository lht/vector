use std::fmt::Debug;

use super::{aggregation::KplAggregator, record::KinesisStreamRecord};
use super::super::sink::{KinesisSink, BatchKinesisRequest};
use crate::sinks::{
    prelude::*,
    util::StreamSink,
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
        _aggregator: KplAggregator,
    ) -> Result<(), ()> {
        // TODO: Implement proper aggregation pipeline
        // For now, delegate to base sink (will be implemented in next iteration)
        Box::new(self.base_sink).run_inner(input).await
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