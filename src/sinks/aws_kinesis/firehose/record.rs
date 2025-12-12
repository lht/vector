use aws_sdk_firehose::operation::put_record_batch::PutRecordBatchOutput;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::Blob;
use bytes::Bytes;
use tracing::Instrument;

use super::{KinesisClient, KinesisError, KinesisRecord, KinesisResponse, Record, SendRecord};
use crate::sinks::prelude::*;

#[derive(Clone)]
pub struct KinesisFirehoseRecord {
    pub record: KinesisRecord,
}

impl Record for KinesisFirehoseRecord {
    type T = KinesisRecord;

    fn new(payload_bytes: &Bytes, _partition_key: &str) -> Self {
        Self {
            record: KinesisRecord::builder()
                .data(Blob::new(&payload_bytes[..]))
                .build()
                .expect("all builder records specified"),
        }
    }

    fn encoded_length(&self) -> usize {
        let data_len = self.record.data.as_ref().len();
        // data is simply base64 encoded, quoted, and comma separated
        data_len.div_ceil(3) * 4 + 3
    }

    fn get(self) -> Self::T {
        self.record
    }
}

#[derive(Clone)]
pub struct KinesisFirehoseClient {
    pub client: KinesisClient,
}

impl SendRecord for KinesisFirehoseClient {
    type T = KinesisRecord;
    type E = KinesisError;

    async fn send(
        &self,
        records: Vec<Self::T>,
        stream_name: String,
    ) -> Result<
        KinesisResponse,
        SdkError<Self::E, aws_smithy_runtime_api::client::orchestrator::HttpResponse>,
    > {
        self.client
            .put_record_batch()
            .set_records(Some(records.clone()))
            .delivery_stream_name(stream_name)
            .send()
            .instrument(info_span!("request").or_current())
            .await
            .map(|output: PutRecordBatchOutput| process_output(output, records))
    }
}

fn process_output(output: PutRecordBatchOutput, records: Vec<KinesisRecord>) -> KinesisResponse {
    let failure_count = output.failed_put_count() as usize;
    let successful_count = records.len() - failure_count;

    // Calculate successful byte size using zip (AWS guarantees "natural ordering")
    let successful_size: usize = records
        .iter()
        .zip(output.request_responses().iter())
        .filter_map(|(record, response)| {
            if response.error_code().is_none() {
                Some(record.data().as_ref().len())
            } else {
                None
            }
        })
        .sum();

    KinesisResponse {
        failure_count,
        events_byte_size: CountByteSize(successful_count, JsonSize::new(successful_size)).into(),
        #[cfg(feature = "sinks-aws_kinesis_streams")]
        failed_records: vec![], // Firehose doesn't support partial failure retry
    }
}
