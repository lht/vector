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

    fn from_aggregated(aggregated: &super::super::aggregation::AggregatedRecord) -> Self {
        Self {
            record: KinesisRecord::builder()
                .data(Blob::new(&aggregated.data[..]))
                .partition_key(&aggregated.partition_key)
                .build()
                .expect("all required builder fields set"),
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)] // Reserved for future aggregated record handling
pub struct KinesisAggregatedStreamRecord {
    pub record: KinesisRecord,
    pub user_record_count: usize,
}

impl Record for KinesisAggregatedStreamRecord {
    type T = KinesisRecord;

    fn new(_payload_bytes: &Bytes, _partition_key: &str) -> Self {
        // This shouldn't be called for aggregated records, use from_aggregated instead
        panic!("Use from_aggregated for aggregated records, not new")
    }

    fn from_aggregated(aggregated: &super::super::aggregation::AggregatedRecord) -> Self {
        Self {
            record: KinesisRecord::builder()
                .data(Blob::new(&aggregated.data[..]))
                .partition_key(&aggregated.partition_key)
                .build()
                .expect("all required builder fields set"),
            user_record_count: aggregated.user_record_count,
        }
    }

    fn encoded_length(&self) -> usize {
        // For aggregated records, return the actual data length
        // since the KPL format is already optimized
        self.record.data.as_ref().len() + self.record.partition_key.len() + 10
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
        let rec_count = records.len();
        let total_size = records
            .iter()
            .fold(0, |acc, record| acc + record.data().as_ref().len());

        self.client
            .put_records()
            .set_records(Some(records))
            .stream_name(stream_name)
            .send()
            .instrument(info_span!("request").or_current())
            .await
            .map(|output: PutRecordsOutput| KinesisResponse {
                failure_count: output.failed_record_count().unwrap_or(0) as usize,
                events_byte_size: CountByteSize(rec_count, JsonSize::new(total_size)).into(),
            })
    }
}
