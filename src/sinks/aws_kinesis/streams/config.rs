use aws_sdk_kinesis::operation::{
    describe_stream::DescribeStreamError, put_records::PutRecordsError,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use futures::FutureExt;
use snafu::Snafu;
use vector_lib::configurable::{component::GenerateConfig, configurable_component};

use super::aggregation::KplAggregator;
use super::sink::KinesisStreamsSink;
use super::{
    KinesisClient, KinesisError, KinesisRecord, KinesisResponse, KinesisSinkBaseConfig,
    base_sink::{BatchKinesisRequest, KinesisSink},
    record::{KinesisStreamClient, KinesisStreamRecord},
    request_builder::KinesisRequestBuilder,
};
use crate::{
    aws::{ClientBuilder, create_client, is_retriable_error},
    codecs::Encoder,
    config::{AcknowledgementsConfig, Input, ProxyConfig, SinkConfig, SinkContext},
    sinks::{
        Healthcheck, VectorSink,
        prelude::ServiceBuilder,
        prelude::*,
        util::{
            BatchConfig, SinkBatchSettings,
            retries::{RetryAction, RetryLogic},
            service::ServiceBuilderExt,
        },
    },
};
use std::marker::PhantomData;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
enum HealthcheckError {
    #[snafu(display("DescribeStream failed: {}", source))]
    DescribeStreamFailed {
        source: SdkError<DescribeStreamError, HttpResponse>,
    },
    #[snafu(display("Stream names do not match, got {}, expected {}", name, stream_name))]
    StreamNamesMismatch { name: String, stream_name: String },
}

pub struct KinesisClientBuilder;

impl ClientBuilder for KinesisClientBuilder {
    type Client = KinesisClient;

    fn build(&self, config: &aws_types::SdkConfig) -> Self::Client {
        KinesisClient::new(config)
    }
}

pub const MAX_PAYLOAD_SIZE: usize = 5_000_000;
pub const MAX_PAYLOAD_EVENTS: usize = 500;

#[derive(Clone, Copy, Debug, Default)]
pub struct KinesisDefaultBatchSettings;

impl SinkBatchSettings for KinesisDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(MAX_PAYLOAD_EVENTS);
    const MAX_BYTES: Option<usize> = Some(MAX_PAYLOAD_SIZE);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Default value for max_events in KPL aggregation
const fn default_kpl_max_events() -> usize {
    100
}

/// Configuration for KPL (Kinesis Producer Library) aggregation.
///
/// When enabled, multiple user records (KPL user records) are packed into single
/// Kinesis Data Streams records to improve throughput and reduce API calls.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(deny_unknown_fields)]
pub struct AggregationConfig {
    /// Enable KPL aggregation.
    ///
    /// When enabled, multiple user records are packed into single Kinesis Data Streams records
    /// to improve throughput and reduce API calls. Records are aggregated up to the
    /// specified limits, with the first record's partition key used for the entire
    /// aggregate. This may cause records with different partition keys to be routed
    /// to the same shard.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of user records to aggregate into a single Kinesis Data Streams record.
    ///
    /// Higher values improve throughput but increase latency and retry payload size.
    /// Must be between 1 and 1000.
    #[serde(default = "default_kpl_max_events")]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub max_events: usize,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_events: default_kpl_max_events(),
        }
    }
}

impl AggregationConfig {
    /// Validates the KPL aggregation configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.enabled && (self.max_events == 0 || self.max_events > 1000) {
            return Err("aggregation.max_events must be between 1 and 1000".into());
        }
        Ok(())
    }
}

/// Configuration for the `aws_kinesis_streams` sink.
#[configurable_component(sink(
    "aws_kinesis_streams",
    "Publish logs to AWS Kinesis Streams topics."
))]
#[derive(Clone, Debug)]
pub struct KinesisStreamsSinkConfig {
    #[serde(flatten)]
    pub base: KinesisSinkBaseConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<KinesisDefaultBatchSettings>,

    /// KPL aggregation configuration.
    #[configurable(derived)]
    #[serde(default)]
    pub aggregation: AggregationConfig,
}

impl KinesisStreamsSinkConfig {
    async fn healthcheck(self, client: KinesisClient) -> crate::Result<()> {
        let stream_name = self.base.stream_name;

        let describe_result = client
            .describe_stream()
            .stream_name(stream_name.clone())
            .set_exclusive_start_shard_id(None)
            .limit(1)
            .send()
            .await;

        match describe_result {
            Ok(resp) => {
                let name = resp
                    .stream_description
                    .map(|x| x.stream_name)
                    .unwrap_or_default();
                if name == stream_name {
                    Ok(())
                } else {
                    Err(HealthcheckError::StreamNamesMismatch { name, stream_name }.into())
                }
            }
            Err(source) => Err(HealthcheckError::DescribeStreamFailed { source }.into()),
        }
    }

    pub async fn create_client(&self, proxy: &ProxyConfig) -> crate::Result<KinesisClient> {
        create_client::<KinesisClientBuilder>(
            &KinesisClientBuilder {},
            &self.base.auth,
            self.base.region.region(),
            self.base.region.endpoint(),
            proxy,
            self.base.tls.as_ref(),
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "aws_kinesis_streams")]
impl SinkConfig for KinesisStreamsSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        // Validate KPL aggregation config
        self.aggregation.validate()?;

        let client = self.create_client(&cx.proxy).await?;
        let healthcheck = self.clone().healthcheck(client.clone()).boxed();

        let batch_settings = self
            .batch
            .validate()?
            .limit_max_bytes(MAX_PAYLOAD_SIZE)?
            .limit_max_events(MAX_PAYLOAD_EVENTS)?
            .into_batcher_settings()?;

        // Build sink with aggregation support
        let request_limits = self.base.request.into_settings();
        let region = self.base.region.region();

        let service = ServiceBuilder::new()
            .settings::<KinesisRetryLogic, BatchKinesisRequest<KinesisStreamRecord>>(
                request_limits,
                KinesisRetryLogic {
                    retry_partial: self.base.request_retry_partial,
                },
            )
            .service(
                super::KinesisService::<KinesisStreamClient, KinesisRecord, KinesisError> {
                    client: KinesisStreamClient { client },
                    stream_name: self.base.stream_name.clone(),
                    region,
                    _phantom_t: PhantomData,
                    _phantom_e: PhantomData,
                },
            );

        let transformer = self.base.encoding.transformer();
        let serializer = self.base.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);

        let aggregator = if self.aggregation.enabled {
            Some(KplAggregator::new(self.aggregation.max_events))
        } else {
            None
        };

        let request_builder = KinesisRequestBuilder::<KinesisStreamRecord> {
            compression: self.base.compression,
            encoder: (transformer, encoder),
            _phantom: PhantomData,
        };

        let base_sink = KinesisSink {
            batch_settings,
            service,
            request_builder,
            partition_key_field: self.base.partition_key_field.clone(),
            _phantom: PhantomData,
        };

        let streams_sink = KinesisStreamsSink {
            base_sink,
            aggregator,
        };

        Ok((VectorSink::from_event_streamsink(streams_sink), healthcheck))
    }

    fn input(&self) -> Input {
        self.base.input()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        self.base.acknowledgements()
    }
}

impl GenerateConfig for KinesisStreamsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"partition_key_field = "foo"
            stream_name = "my-stream"
            encoding.codec = "json""#,
        )
        .unwrap()
    }
}
#[derive(Default, Clone)]
struct KinesisRetryLogic {
    retry_partial: bool,
}

impl RetryLogic for KinesisRetryLogic {
    type Error = SdkError<KinesisError, HttpResponse>;
    type Request = BatchKinesisRequest<KinesisStreamRecord>;
    type Response = KinesisResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        if let SdkError::ServiceError(inner) = error {
            // Note that if the request partially fails (records sent to one
            // partition fail but the others do not, for example), Vector
            // does not retry. This line only covers a failure for the entire
            // request.
            //
            // https://github.com/vectordotdev/vector/issues/359
            if matches!(
                inner.err(),
                PutRecordsError::ProvisionedThroughputExceededException(_)
            ) {
                return true;
            }
        }
        is_retriable_error(error)
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction<Self::Request> {
        if response.failure_count > 0 && self.retry_partial && !response.failed_records.is_empty() {
            let failed_records = response.failed_records.clone();

            RetryAction::RetryPartial(Box::new(move |original_request| {
                let failed_events: Vec<_> = failed_records
                    .iter()
                    .filter_map(|r| original_request.events.get(r.index).cloned())
                    .collect();

                let mut metadata = RequestMetadata::from_batch(
                    failed_events.iter().map(|req| req.get_metadata().clone()),
                );

                // Preserve accumulated counters from the original request
                metadata.accumulate_success(
                    original_request.metadata.accumulated_successful_events(),
                    original_request.metadata.accumulated_successful_bytes(),
                );

                // Build a HashSet of failed indices for O(1) lookup
                use std::collections::HashSet;
                let failed_indices: HashSet<usize> =
                    failed_records.iter().map(|fr| fr.index).collect();

                // Calculate successful events byte size from the original request
                // Use failed_records indices to determine which events succeeded
                let successful_size: usize = original_request
                    .events
                    .iter()
                    .enumerate()
                    .filter_map(|(index, req)| {
                        if failed_indices.contains(&index) {
                            None
                        } else {
                            let size = match req
                                .get_metadata()
                                .events_estimated_json_encoded_byte_size()
                            {
                                GroupedCountByteSize::Untagged { size } => size.1.get(),
                                GroupedCountByteSize::Tagged { .. } => 0,
                            };
                            Some(size)
                        }
                    })
                    .sum();

                // Calculate successful count by summing user_record_count for successful events.
                // This properly accounts for KPL aggregated records which may contain multiple
                // KPL user records per Kinesis Data Streams record.
                let successful_count: usize = original_request
                    .events
                    .iter()
                    .enumerate()
                    .filter_map(|(index, req)| {
                        if failed_indices.contains(&index) {
                            None
                        } else {
                            Some(req.record.user_record_count)
                        }
                    })
                    .sum();

                // Accumulate the successful events from this attempt into the retry metadata
                metadata.accumulate_success(successful_count, successful_size);

                BatchKinesisRequest {
                    events: failed_events,
                    metadata,
                }
            }))
        } else {
            RetryAction::Successful
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::{
        aws_kinesis::{
            request_builder::{KinesisMetadata, KinesisRequest, KinesisRequestBuilder},
            service::RecordResult,
        },
        util::{Compression, request_builder::EncodeResult},
    };
    use std::marker::PhantomData;
    use vector_lib::{internal_event::CountByteSize, json_size::JsonSize};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<KinesisStreamsSinkConfig>();
    }

    // Helper function to create a KinesisRequest for testing
    fn create_test_kinesis_request(
        index: usize,
        byte_size: usize,
    ) -> KinesisRequest<KinesisStreamRecord> {
        let metadata = RequestMetadata::new(
            1,
            byte_size,
            byte_size,
            byte_size,
            CountByteSize(1, JsonSize::new(byte_size)).into(),
        );

        // Use the KinesisRequestBuilder approach to construct properly
        KinesisRequestBuilder::<KinesisStreamRecord> {
            compression: Compression::None,
            encoder: (Default::default(), Default::default()),
            _phantom: PhantomData,
        }
        .build_request(
            KinesisMetadata {
                finalizers: Default::default(),
                partition_key: format!("key-{}", index),
            },
            metadata,
            EncodeResult::uncompressed(
                bytes::Bytes::from(vec![index as u8; byte_size]),
                CountByteSize(1, JsonSize::new(byte_size)).into(),
            ),
        )
    }

    #[test]
    fn test_accumulated_counters_preserved_during_retries() {
        // This test verifies that when partial failures occur and retries happen,
        // the successful event counts and byte sizes from previous attempts are
        // accumulated correctly in the retry request metadata.
        //
        // Scenario:
        // - Initial request: 10 events (1000 bytes total, 100 bytes each)
        // - First attempt: 7 succeed, 3 fail (indices 2, 5, 8)
        // - Retry request should:
        //   1. Only contain the 3 failed events
        //   2. Accumulate 7 successful events (700 bytes) in metadata
        // - Second attempt: 2 succeed, 1 fails (index 1 of retry, which is original index 5)
        // - Second retry request should:
        //   1. Only contain the 1 failed event
        //   2. Accumulate 9 successful events (900 bytes) total
        //
        // Note: Each KinesisStreamRecord has user_record_count=1 (non-aggregated)

        let retry_logic = KinesisRetryLogic {
            retry_partial: true,
        };

        // Create initial request with 10 events (100 bytes each)
        let initial_events: Vec<KinesisRequest<KinesisStreamRecord>> = (0..10)
            .map(|i| create_test_kinesis_request(i, 100))
            .collect();

        let initial_metadata = RequestMetadata::from_batch(
            initial_events.iter().map(|req| req.get_metadata().clone()),
        );

        let initial_request = BatchKinesisRequest {
            events: initial_events,
            metadata: initial_metadata,
        };

        // Verify initial request has no accumulated counters
        assert_eq!(
            initial_request.metadata.accumulated_successful_events(),
            0,
            "Initial request should have no accumulated successful events"
        );
        assert_eq!(
            initial_request.metadata.accumulated_successful_bytes(),
            0,
            "Initial request should have no accumulated successful bytes"
        );

        // First response: 7 succeed, 3 fail (indices 2, 5, 8 fail)
        let first_response = KinesisResponse {
            failure_count: 3,
            events_byte_size: CountByteSize(7, JsonSize::new(700)).into(),
            failed_records: vec![
                RecordResult {
                    index: 2,
                    success: false,
                    error_code: Some("ProvisionedThroughputExceededException".to_string()),
                    error_message: Some("Rate exceeded".to_string()),
                },
                RecordResult {
                    index: 5,
                    success: false,
                    error_code: Some("ProvisionedThroughputExceededException".to_string()),
                    error_message: Some("Rate exceeded".to_string()),
                },
                RecordResult {
                    index: 8,
                    success: false,
                    error_code: Some("InternalFailure".to_string()),
                    error_message: Some("Internal error".to_string()),
                },
            ],
        };

        // Get retry action for first attempt
        let retry_action = retry_logic.should_retry_response(&first_response);

        match retry_action {
            RetryAction::RetryPartial(retry_fn) => {
                let first_retry_request = retry_fn(initial_request);

                // Verify first retry request contains only 3 failed events
                assert_eq!(
                    first_retry_request.events.len(),
                    3,
                    "First retry should contain 3 failed events"
                );

                // Verify accumulated counters from first attempt
                assert_eq!(
                    first_retry_request.metadata.accumulated_successful_events(),
                    7,
                    "First retry should accumulate 7 successful events from first attempt"
                );
                assert_eq!(
                    first_retry_request.metadata.accumulated_successful_bytes(),
                    700,
                    "First retry should accumulate 700 bytes from first attempt"
                );

                // Verify the retry request has the correct failed events (indices 2, 5, 8)
                let retry_partition_keys: Vec<String> = first_retry_request
                    .events
                    .iter()
                    .map(|req| req.key.partition_key.clone())
                    .collect();
                assert_eq!(
                    retry_partition_keys,
                    vec!["key-2", "key-5", "key-8"],
                    "First retry should contain events at original indices 2, 5, 8"
                );

                // Second response: 2 succeed, 1 fails (index 1 of the retry, which is original index 5)
                let second_response = KinesisResponse {
                    failure_count: 1,
                    events_byte_size: CountByteSize(2, JsonSize::new(200)).into(),
                    failed_records: vec![RecordResult {
                        index: 1, // This is index 1 in the retry request (original index 5)
                        success: false,
                        error_code: Some("ProvisionedThroughputExceededException".to_string()),
                        error_message: Some("Rate still exceeded".to_string()),
                    }],
                };

                // Get retry action for second attempt
                let second_retry_action = retry_logic.should_retry_response(&second_response);

                match second_retry_action {
                    RetryAction::RetryPartial(second_retry_fn) => {
                        let second_retry_request = second_retry_fn(first_retry_request);

                        // Verify second retry request contains only 1 failed event
                        assert_eq!(
                            second_retry_request.events.len(),
                            1,
                            "Second retry should contain 1 failed event"
                        );

                        // Verify accumulated counters now include both attempts
                        // First attempt: 7 successful
                        // Second attempt: 2 successful
                        // Total: 9 successful events, 900 bytes
                        assert_eq!(
                            second_retry_request
                                .metadata
                                .accumulated_successful_events(),
                            9,
                            "Second retry should accumulate 9 successful events total (7 from first + 2 from second)"
                        );
                        assert_eq!(
                            second_retry_request.metadata.accumulated_successful_bytes(),
                            900,
                            "Second retry should accumulate 900 bytes total (700 from first + 200 from second)"
                        );

                        // Verify the second retry request has the correct failed event (original index 5)
                        let second_retry_partition_keys: Vec<String> = second_retry_request
                            .events
                            .iter()
                            .map(|req| req.key.partition_key.clone())
                            .collect();
                        assert_eq!(
                            second_retry_partition_keys,
                            vec!["key-5"],
                            "Second retry should contain event at original index 5"
                        );
                    }
                    _ => panic!("Expected RetryPartial action for second attempt"),
                }
            }
            _ => panic!("Expected RetryPartial action for first attempt"),
        }
    }

    #[test]
    fn test_kpl_aggregated_records_user_count() {
        // This test verifies that KPL aggregated records properly track user_record_count
        // and that successful count calculation accounts for multiple KPL user records per
        // Kinesis Data Streams record.
        //
        // Scenario:
        // - Initial request: 3 Kinesis Data Streams records (aggregated) containing 2, 3, and 5 KPL user records respectively
        // - First attempt: 2 Kinesis records succeed (containing 2+3=5 user records), 1 fails (containing 5 user records)
        // - Retry request should accumulate 5 successful KPL user records (not 2 Kinesis Data Streams records)

        let retry_logic = KinesisRetryLogic {
            retry_partial: true,
        };

        // Helper to create a KinesisRequest with custom user_record_count
        fn create_aggregated_kinesis_request(
            index: usize,
            byte_size: usize,
            user_record_count: usize,
        ) -> KinesisRequest<KinesisStreamRecord> {
            let metadata = RequestMetadata::new(
                1, // metadata tracks 1 Kinesis record
                byte_size,
                byte_size,
                byte_size,
                CountByteSize(1, JsonSize::new(byte_size)).into(),
            );

            let mut request = KinesisRequestBuilder::<KinesisStreamRecord> {
                compression: Compression::None,
                encoder: (Default::default(), Default::default()),
                _phantom: PhantomData,
            }
            .build_request(
                KinesisMetadata {
                    finalizers: Default::default(),
                    partition_key: format!("key-{}", index),
                },
                metadata,
                EncodeResult::uncompressed(
                    bytes::Bytes::from(vec![index as u8; byte_size]),
                    CountByteSize(1, JsonSize::new(byte_size)).into(),
                ),
            );

            // Override user_record_count to simulate KPL aggregation
            request.record.user_record_count = user_record_count;
            request
        }

        // Create request with 3 aggregated Kinesis Data Streams records:
        // - Record 0: contains 2 KPL user records, 200 bytes
        // - Record 1: contains 3 KPL user records, 300 bytes
        // - Record 2: contains 5 KPL user records, 500 bytes
        // Total: 10 KPL user records across 3 Kinesis Data Streams records
        let initial_events = vec![
            create_aggregated_kinesis_request(0, 200, 2),
            create_aggregated_kinesis_request(1, 300, 3),
            create_aggregated_kinesis_request(2, 500, 5),
        ];

        let initial_metadata = RequestMetadata::from_batch(
            initial_events.iter().map(|req| req.get_metadata().clone()),
        );

        let initial_request = BatchKinesisRequest {
            events: initial_events,
            metadata: initial_metadata,
        };

        // First response: records 0 and 1 succeed (5 user records), record 2 fails (5 user records)
        let first_response = KinesisResponse {
            failure_count: 1,
            events_byte_size: CountByteSize(5, JsonSize::new(500)).into(), // 5 user records succeeded (2+3)
            failed_records: vec![RecordResult {
                index: 2,
                success: false,
                error_code: Some("ProvisionedThroughputExceededException".to_string()),
                error_message: Some("Rate exceeded".to_string()),
            }],
        };

        let retry_action = retry_logic.should_retry_response(&first_response);

        match retry_action {
            RetryAction::RetryPartial(retry_fn) => {
                let retry_request = retry_fn(initial_request);

                // Verify retry request contains only 1 failed Kinesis record
                assert_eq!(
                    retry_request.events.len(),
                    1,
                    "Retry should contain 1 failed Kinesis record"
                );

                // Verify the failed record has the correct user_record_count
                assert_eq!(
                    retry_request.events[0].record.user_record_count, 5,
                    "Failed record should contain 5 user records"
                );

                // Verify accumulated counters account for user records, not Kinesis records
                // Should accumulate 5 user records (2+3), not 2 Kinesis records
                assert_eq!(
                    retry_request.metadata.accumulated_successful_events(),
                    5,
                    "Should accumulate 5 successful user records (2+3), not 2 Kinesis records"
                );

                // Verify accumulated bytes (200+300=500)
                assert_eq!(
                    retry_request.metadata.accumulated_successful_bytes(),
                    500,
                    "Should accumulate 500 bytes from successful records"
                );
            }
            _ => panic!("Expected RetryPartial action"),
        }
    }
}
