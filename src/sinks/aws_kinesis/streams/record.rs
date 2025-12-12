use aws_sdk_kinesis::operation::put_records::PutRecordsOutput;
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_smithy_types::Blob;
use bytes::Bytes;
use tracing::Instrument;

use super::{KinesisClient, KinesisError, KinesisRecord, KinesisResponse, Record, SendRecord};
use crate::sinks::prelude::*;

#[derive(Clone)]
pub struct KinesisStreamRecord {
    pub record: KinesisRecord,
}

impl Record for KinesisStreamRecord {
    type T = KinesisRecord;

    fn new(payload_bytes: &Bytes, partition_key: &str) -> Self {
        Self {
            record: KinesisRecord::builder()
                .data(Blob::new(&payload_bytes[..]))
                .partition_key(partition_key)
                .build()
                .expect("all required builder fields set"),
        }
    }

    fn encoded_length(&self) -> usize {
        let hash_key_size = self
            .record
            .explicit_hash_key
            .as_ref()
            .map(|s| s.len())
            .unwrap_or_default();

        // data is base64 encoded
        let data_len = self.record.data.as_ref().len();
        let key_len = self.record.partition_key.len();

        data_len.div_ceil(3) * 4 + hash_key_size + key_len + 10
    }

    fn get(self) -> Self::T {
        self.record
    }
}

#[derive(Clone)]
pub struct KinesisStreamClient {
    pub client: KinesisClient,
}

impl SendRecord for KinesisStreamClient {
    type T = KinesisRecord;
    type E = KinesisError;

    async fn send(
        &self,
        records: Vec<Self::T>,
        stream_name: String,
    ) -> Result<KinesisResponse, SdkError<Self::E, HttpResponse>> {
        self.client
            .put_records()
            .set_records(Some(records.clone()))
            .stream_name(stream_name)
            .send()
            .instrument(info_span!("request").or_current())
            .await
            .map(|output: PutRecordsOutput| process_output(output, records))
    }
}

fn process_output(output: PutRecordsOutput, records: Vec<KinesisRecord>) -> KinesisResponse {
    // Extract failed records (needed for retry logic)
    let failed_records: Vec<super::super::service::RecordResult> = output
        .records()
        .iter()
        .enumerate()
        .filter_map(|(idx, response)| {
            response
                .error_code()
                .map(|error_code| super::super::service::RecordResult {
                    index: idx,
                    success: false,
                    error_code: Some(error_code.to_string()),
                    error_message: response.error_message().map(String::from),
                })
        })
        .collect();

    let failure_count = failed_records.len();
    let successful_count = records.len() - failure_count;

    // Calculate successful byte size using zip (AWS guarantees "natural ordering")
    let successful_size: usize = records
        .iter()
        .zip(output.records().iter())
        .filter_map(|(record, response)| {
            if response.error_code().is_none() {
                Some(record.data().as_ref().len())
            } else {
                None
            }
        })
        .sum();

    KinesisResponse {
        failed_records,
        failure_count,
        events_byte_size: CountByteSize(successful_count, JsonSize::new(successful_size)).into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_kinesis::{operation::put_records::PutRecordsOutput, types::PutRecordsResultEntry};

    #[test]
    fn test_process_output_all_success() {
        use aws_smithy_types::Blob;

        // Create mock successful records
        let record1 = PutRecordsResultEntry::builder()
            .sequence_number("seq1")
            .shard_id("shard1")
            .build();

        let record2 = PutRecordsResultEntry::builder()
            .sequence_number("seq2")
            .shard_id("shard2")
            .build();

        let output = PutRecordsOutput::builder()
            .records(record1)
            .records(record2)
            .failed_record_count(0)
            .build()
            .unwrap();

        let mock_records = vec![
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key0")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key1")
                .build()
                .unwrap(),
        ];

        let response = process_output(output, mock_records);

        // Should have no failures
        assert_eq!(response.failed_records.len(), 0);
        assert_eq!(response.failure_count, 0);
    }

    #[test]
    fn test_process_output_mixed_success_failure() {
        use aws_smithy_types::Blob;

        // Create mock records with mixed success/failure
        let success_record = PutRecordsResultEntry::builder()
            .sequence_number("seq1")
            .shard_id("shard1")
            .build();

        let failure_record = PutRecordsResultEntry::builder()
            .error_code("ProvisionedThroughputExceededException")
            .error_message("Rate exceeded for shard")
            .build();

        let output = PutRecordsOutput::builder()
            .records(success_record)
            .records(failure_record)
            .failed_record_count(1)
            .build()
            .unwrap();

        let mock_records = vec![
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key0")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key1")
                .build()
                .unwrap(),
        ];

        let response = process_output(output, mock_records);

        // Should only return the failed record
        assert_eq!(response.failed_records.len(), 1);
        assert_eq!(response.failure_count, 1);

        // Only record should be the failed one (originally at index 1)
        assert!(!response.failed_records[0].success);
        assert_eq!(
            response.failed_records[0].error_code.as_ref().unwrap(),
            "ProvisionedThroughputExceededException"
        );
        assert_eq!(
            response.failed_records[0].error_message.as_ref().unwrap(),
            "Rate exceeded for shard"
        );
        assert_eq!(response.failed_records[0].index, 1);
    }

    #[test]
    fn test_process_output_all_failures() {
        use aws_smithy_types::Blob;

        // Create mock failed records
        let failure_record1 = PutRecordsResultEntry::builder()
            .error_code("ProvisionedThroughputExceededException")
            .error_message("Rate exceeded")
            .build();

        let failure_record2 = PutRecordsResultEntry::builder()
            .error_code("InternalFailure")
            .error_message("Internal server error")
            .build();

        let output = PutRecordsOutput::builder()
            .records(failure_record1)
            .records(failure_record2)
            .failed_record_count(2)
            .build()
            .unwrap();

        let mock_records = vec![
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key0")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key1")
                .build()
                .unwrap(),
        ];

        let response = process_output(output, mock_records);

        assert_eq!(response.failed_records.len(), 2);
        assert_eq!(response.failure_count, 2);
        assert!(!response.failed_records[0].success);
        assert!(!response.failed_records[1].success);
        assert_eq!(
            response.failed_records[0].error_code.as_ref().unwrap(),
            "ProvisionedThroughputExceededException"
        );
        assert_eq!(
            response.failed_records[1].error_code.as_ref().unwrap(),
            "InternalFailure"
        );
    }

    #[test]
    fn test_kinesis_response_metrics_with_partial_failure() {
        use aws_smithy_types::Blob;

        // Create a mock response with 100 records where 20 failed
        let mut output_builder = PutRecordsOutput::builder().failed_record_count(20);

        // Add 80 successful records
        for i in 0..80 {
            output_builder = output_builder.records(
                PutRecordsResultEntry::builder()
                    .sequence_number(format!("seq{}", i))
                    .shard_id("shard1")
                    .build(),
            );
        }

        // Add 20 failed records
        for _ in 0..20 {
            output_builder = output_builder.records(
                PutRecordsResultEntry::builder()
                    .error_code("ProvisionedThroughputExceededException")
                    .error_message("Rate exceeded")
                    .build(),
            );
        }

        let output = output_builder.build().unwrap();

        // Simulate the metrics calculation with 100 records of 1000 bytes each
        let rec_count = 100;

        // Create mock records with 1000 bytes of data each
        let mock_records: Vec<KinesisRecord> = (0..100)
            .map(|i| {
                KinesisRecord::builder()
                    .data(Blob::new(vec![0u8; 1000]))
                    .partition_key(format!("key{}", i))
                    .build()
                    .unwrap()
            })
            .collect();

        let response = process_output(output, mock_records.clone());

        let failure_count = response.failure_count;
        let successful_count = rec_count - failure_count;
        let successful_size = response.events_byte_size.size().unwrap().1.get() as usize;

        // Verify the metrics reflect only successful events
        assert_eq!(successful_count, 80, "Should count only successful events");
        assert_eq!(
            successful_size, 80000,
            "Should calculate accurate byte size for successful events"
        );
        assert_eq!(
            response.failed_records.len(),
            20,
            "Should track all failed records"
        );
        assert_eq!(failure_count, 20, "Should report correct failure count");
    }

    #[test]
    fn test_accurate_size_calculation_with_varying_record_sizes() {
        // Test that we calculate accurate sizes even when records have different sizes
        // This verifies that we're not using proportional calculation

        // Create a mock response where larger records failed
        let mut output_builder = PutRecordsOutput::builder().failed_record_count(2);

        // Add 3 successful records
        for i in 0..3 {
            output_builder = output_builder.records(
                PutRecordsResultEntry::builder()
                    .sequence_number(format!("seq{}", i))
                    .shard_id("shard1")
                    .build(),
            );
        }

        // Add 2 failed records
        for _ in 0..2 {
            output_builder = output_builder.records(
                PutRecordsResultEntry::builder()
                    .error_code("ProvisionedThroughputExceededException")
                    .error_message("Rate exceeded")
                    .build(),
            );
        }

        let output = output_builder.build().unwrap();

        // Simulate records with varying sizes:
        // First 3 records (successful) are small: 100 bytes each = 300 bytes total
        // Last 2 records (failed) are large: 5000 bytes each = 10000 bytes total
        let mock_records: Vec<KinesisRecord> = vec![
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key0")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key1")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 100]))
                .partition_key("key2")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 5000]))
                .partition_key("key3")
                .build()
                .unwrap(),
            KinesisRecord::builder()
                .data(Blob::new(vec![0u8; 5000]))
                .partition_key("key4")
                .build()
                .unwrap(),
        ];
        let rec_count = 5;

        let response = process_output(output, mock_records.clone());

        let failure_count = response.failure_count;
        let successful_count = rec_count - failure_count;
        let successful_size = response.events_byte_size.size().unwrap().1.get() as usize;

        // Verify accurate calculation
        assert_eq!(successful_count, 3, "Should count only successful events");
        assert_eq!(
            successful_size, 300,
            "Should calculate accurate byte size (100+100+100), not proportional"
        );
        assert_eq!(
            response.failed_records.len(),
            2,
            "Should track all failed records"
        );

        // Demonstrate the old proportional method would have been wrong:
        let total_size: usize = mock_records.iter().map(|r| r.data().as_ref().len()).sum();
        let proportional_size = (total_size * successful_count) / rec_count;
        assert_eq!(
            proportional_size, 6180,
            "Proportional calculation would be: (10300 * 3) / 5 = 6180"
        );
        assert_ne!(
            successful_size, proportional_size,
            "Accurate size (300) should differ from proportional (6180)"
        );
    }
}
