use std::marker::PhantomData;

use vector_lib::{lookup::lookup_v2::ConfigValuePath, stream::BatcherSettings};

use super::{
    KinesisResponse, KinesisService,
    record::{Record, SendRecord},
    request_builder::{KinesisRequestBuilder, KinesisAggregationRequestBuilder},
    sink::{BatchKinesisRequest, KinesisSink},
    aggregation::KplAggregator,
};
use crate::{
    aws::{AwsAuthentication, RegionOrEndpoint},
    sinks::{
        prelude::*,
        util::{TowerRequestConfig, retries::RetryLogic},
    },
};

/// Base configuration for the `aws_kinesis_` sinks.
/// The actual specific sink configuration types should either wrap this in a newtype wrapper,
/// or should extend it in a new struct with `serde(flatten)`.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct KinesisSinkBaseConfig {
    /// The [stream name][stream_name] of the target Kinesis Firehose delivery stream.
    ///
    /// [stream_name]: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/Working-with-log-groups-and-streams.html
    #[configurable(metadata(docs::examples = "my-stream"))]
    pub stream_name: String,

    #[serde(flatten)]
    #[configurable(derived)]
    pub region: RegionOrEndpoint,

    #[configurable(derived)]
    pub encoding: EncodingConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(default)]
    pub auth: AwsAuthentication,

    /// Whether or not to retry successful requests containing partial failures.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub request_retry_partial: bool,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// The log field used as the Kinesis record’s partition key value.
    ///
    /// If not specified, a unique partition key is generated for each Kinesis record.
    #[configurable(metadata(docs::examples = "user_id"))]
    pub partition_key_field: Option<ConfigValuePath>,

    /// Enable KPL (Kinesis Producer Library) style aggregation.
    ///
    /// When enabled, multiple user records are packed into single Kinesis records
    /// to improve throughput and reduce API calls. Records with the same partition
    /// key will be aggregated together up to the specified limits.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub enable_aggregation: bool,

    /// Maximum number of user records to aggregate into a single Kinesis record.
    ///
    /// Higher values improve throughput but increase latency and retry payload size.
    /// Must be between 1 and 1000. Only used when `enable_aggregation` is true.
    #[serde(default = "default_max_records_per_aggregate")]
    #[configurable(metadata(docs::advanced))]
    pub max_records_per_aggregate: usize,
}

fn default_max_records_per_aggregate() -> usize {
    100
}

impl KinesisSinkBaseConfig {
    pub fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type() & DataType::Log)
    }

    pub const fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }

    pub fn validate_aggregation_config(&self) -> crate::Result<()> {
        if self.enable_aggregation {
            if self.max_records_per_aggregate == 0 || self.max_records_per_aggregate > 1000 {
                return Err("max_records_per_aggregate must be between 1 and 1000".into());
            }
        }
        Ok(())
    }
}

/// Builds an aws_kinesis sink.
pub fn build_sink<C, R, RR, E, RT>(
    config: &KinesisSinkBaseConfig,
    partition_key_field: Option<ConfigValuePath>,
    batch_settings: BatcherSettings,
    client: C,
    retry_logic: RT,
) -> crate::Result<VectorSink>
where
    C: SendRecord + Clone + Send + Sync + 'static,
    <C as SendRecord>::T: Send,
    <C as SendRecord>::E: Send + Sync + snafu::Error,
    Vec<<C as SendRecord>::T>: FromIterator<R>,
    R: Send + 'static,
    RR: Record + Record<T = R> + Clone + Send + Sync + Unpin + 'static,
    E: Send + 'static,
    RT: RetryLogic<Request = BatchKinesisRequest<RR>, Response = KinesisResponse> + Default,
{
    // Validate aggregation config
    config.validate_aggregation_config()?;
    
    let request_limits = config.request.into_settings();
    let region = config.region.region();
    
    let service = ServiceBuilder::new()
        .settings::<RT, BatchKinesisRequest<RR>>(request_limits, retry_logic)
        .service(KinesisService::<C, R, E> {
            client,
            stream_name: config.stream_name.clone(),
            region,
            _phantom_t: PhantomData,
            _phantom_e: PhantomData,
        });

    let transformer = config.encoding.transformer();
    let serializer = config.encoding.build()?;
    let encoder = Encoder::<()>::new(serializer);

    let (aggregator, aggregation_request_builder) = if config.enable_aggregation {
        let aggregator = Some(KplAggregator::new(config.max_records_per_aggregate));
        let agg_builder = Some(KinesisAggregationRequestBuilder::<RR> {
            compression: config.compression,
            encoder: (transformer.clone(), encoder.clone()),
            enable_aggregation: true,
            _phantom: PhantomData,
        });
        (aggregator, agg_builder)
    } else {
        (None, None)
    };

    // Standard request builder for backward compatibility
    let request_builder = KinesisRequestBuilder::<RR> {
        compression: config.compression,
        encoder: (transformer, encoder),
        _phantom: PhantomData,
    };

    let sink = KinesisSink {
        batch_settings,
        service,
        request_builder,
        aggregation_request_builder,
        partition_key_field,
        aggregator,
        _phantom: PhantomData,
    };
    Ok(VectorSink::from_event_streamsink(sink))
}
