//! # Kinesis Producer Library (KPL) Aggregation Format
//!
//! This module implements the KPL aggregation format for AWS Kinesis streams.
//! KPL aggregation allows multiple user records to be packed into a single Kinesis record,
//! improving throughput and reducing costs.
//!
//! ## KPL Aggregation Overview
//!
//! ```text
//! +-------------------------------------------------------------------------------+
//! |                           USER RECORDS (Application)                          |
//! +-------------------------------------------------------------------------------+
//!
//!   UserRecord 1          UserRecord 2          UserRecord 3          UserRecord 4
//! +--------------+      +--------------+      +--------------+      +--------------+
//! | Data: "..."  |      | Data: "..."  |      | Data: "..."  |      | Data: "..."  |
//! | PKey: "user1"|      | PKey: "user2"|      | PKey: "user3"|      | PKey: "user4"|
//! +--------------+      +--------------+      +--------------+      +--------------+
//!        |                     |                     |                     |
//!        +----------+----------+                     +----------+----------+
//!                   |                                           |
//!                   v                                           v
//! +-------------------------------------------------------------------------------+
//! |                        AGGREGATED RECORDS (KPL Format)                        |
//! +-------------------------------------------------------------------------------+
//!
//!         Aggregated Record 1                         Aggregated Record 2
//!     +----------------------------+             +----------------------------+
//!     | +------------------------+ |             | +------------------------+ |
//!     | |  Magic   (4 bytes)     | |             | |  Magic   (4 bytes)     | |
//!     | |     0xF3899AC2         | |             | |     0xF3899AC2         | |
//!     | +------------------------+ |             | +------------------------+ |
//!     | +------------------------+ |             | +------------------------+ |
//!     | |   Protobuf Message     | |             | |   Protobuf Message     | |
//!     | | +--------------------+ | |             | | +--------------------+ | |
//!     | | |partition_key_table | | |             | | |partition_key_table | | |
//!     | | |  ["key_abc123"]    | | |             | | |  ["key_abc789"]    | | |
//!     | | +--------------------+ | |             | | +--------------------+ | |
//!     | | +--------------------+ | |             | | +--------------------+ | |
//!     | | | records[]          | | |             | | | records[]          | | |
//!     | | |  Record 1:         | | |             | | |  Record 3:         | | |
//!     | | |   pk_index: 0      | | |             | | |   pk_index: 0      | | |
//!     | | |   data: "..."      | | |             | | |   data: "..."      | | |
//!     | | |  Record 2:         | | |             | | |  Record 4:         | | |
//!     | | |   pk_index: 0      | | |             | | |   pk_index: 0      | | |
//!     | | |   data: "..."      | | |             | | |   data: "..."      | | |
//!     | | +--------------------+ | |             | | +--------------------+ | |
//!     | +------------------------+ |             | +------------------------+ |
//!     | +----------------------+   |             | +----------------------+   |
//!     | | MD5 Sum of protobuf  |   |             | | MD5 Sum of protobuf  |   |
//!     | |     (16 bytes)       |   |             | |     (16 bytes)       |   |
//!     | +----------------------+   |             | +----------------------+   |
//!     |                            |             |                            |
//!     | Partition Key: "key_abc123"|             | Partition Key: "key_abc789"|
//!     | User Record Count: 2       |             | User Record Count: 2       |
//!     | Size: ~950KB max           |             | Size: ~950KB max           |
//!     +----------------------------+             +----------------------------+
//!            |                                             |
//!            +------------------+--------------------------+
//!                                       |
//!                                       v
//! +-------------------------------------------------------------------------------+
//! |                    BATCHING FOR PUTRECORDS API CALL                           |
//! +-------------------------------------------------------------------------------+
//!
//!           PutRecords API Request (max 500 records or 10MB per request)
//! +------------------------------------------------------------------------------+
//! | StreamName: "my-stream"                                                      |
//! | Records: [                                                                   |
//! |   {                                                                          |
//! |     Data: <Aggregated Record 1 binary>  // Contains 2 user records           |
//! |     PartitionKey: "key_abc123"                                               |
//! |     ExplicitHashKey: "12345..." (optional - for shard control)               |
//! |   },                                                                         |
//! |   {                                                                          |
//! |     Data: <Aggregated Record 2 binary>  // Contains 2 user records           |
//! |     PartitionKey: "key_xyz789"                                               |
//! |     ExplicitHashKey: "67890..." (optional - for shard control)               |
//! |   },                                                                         |
//! |   ... (up to 500 aggregated records)                                         |
//! | ]                                                                            |
//! +------------------------------------------------------------------------------+
//!                                       |
//!                                       v
//! +-------------------------------------------------------------------------------+
//! |                        AWS KINESIS DATA STREAM                                |
//! |                                                                               |
//! |  Shard 1                      Shard 2                      Shard N           |
//! | +---------+                  +---------+                  +---------+        |
//! | | Agg 1   |                  | Agg 2   |                  | ...     |        |
//! | +---------+                  +---------+                  +---------+        |
//! +-------------------------------------------------------------------------------+
//! ```
//!
//! ## Implementation Notes
//!
//! - All records in an aggregate share a single partition key
//! - The partition key table contains exactly one entry
//! - All records reference partition_key_index = 0
//! - explicit_hash_key_table is always empty (not used)
//! - This ensures the aggregated record is correctly associated with its Kinesis shard
//!
//! ## Size Limits
//!
//! - **Max Aggregated Record Size**: 950KB (leaves room for overhead under 1MB AWS limit)
//! - **Max Records per Aggregate**: Configurable
//! - **Max PutRecords Batch**: 500 records or 10MB total per API call
//! - **Individual User Record**: No specific limit, but must fit within aggregate
//!
//! ## References
//!
//! - [AWS KPL Concepts](https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html)
//! - [KPL Aggregation Format](https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md)

use crate::event::{EventFinalizers, Finalizable};
use bytes::{BufMut, Bytes, BytesMut};
use md5::{Digest, Md5};
use prost::Message;
use std::collections::VecDeque;
use vector_lib::request_metadata::RequestMetadata;

// Include the generated protobuf code
pub mod kpl_proto {
    include!(concat!(env!("OUT_DIR"), "/kpl_aggregation.rs"));
}

// KPL Magic bytes - identifies this as a KPL aggregated record
const KPL_MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];

// Maximum size for a Kinesis record (1 MiB)
const MAX_KINESIS_RECORD_SIZE: usize = 1024 * 1024;

// Fixed KPL overhead: magic (4) + MD5 (16)
const KPL_FIXED_OVERHEAD: usize = 20;

// Protobuf base overhead per aggregated record:
//   - partition_key_table: 1 key (16 chars × 4 bytes Unicode = 64) + encoding (2) = 66 bytes
//   - explicit_hash_key_table: empty, just encoding = 2 bytes
//   - Total: 68 bytes, rounded to 80 for safety buffer
const PROTOBUF_BASE_OVERHEAD: usize = 80;

// Protobuf overhead per user record:
//   - Record wrapper: ~2 bytes
//   - partition_key_index field: 2 bytes (always 0)
//   - data field encoding: ~2 bytes
//   - Total: 6 bytes, rounded to 8 for safety buffer
const PROTOBUF_PER_RECORD_OVERHEAD: usize = 8;

/// Individual user record before aggregation
#[derive(Debug, Clone)]
pub struct UserRecord {
    /// Data payload
    pub data: Bytes,
    /// Partition key for this record
    pub partition_key: String,
    /// Optional explicit hash key
    pub explicit_hash_key: Option<String>,
    /// Event finalizers for acknowledgment
    pub finalizers: EventFinalizers,
    /// Request metadata
    pub metadata: RequestMetadata,
}

impl UserRecord {
    /// Calculate the estimated encoded size of this record in protobuf format.
    /// This is an approximation used for buffer size calculations.
    pub fn encoded_size(&self) -> usize {
        // Note: explicit_hash_key is NOT encoded in KPL protobuf (always set to None)
        // Only the data is encoded in the protobuf, plus minimal overhead
        self.data.len() + 20 // data + overhead estimate
    }
}

impl Finalizable for UserRecord {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

/// Aggregated record containing multiple user records
#[derive(Debug, Clone)]
pub struct AggregatedRecord {
    /// KPL-formatted data: magic + records + checksum
    pub data: Bytes,
    /// Partition key from the first user record
    pub partition_key: String,
    /// Number of user records in this aggregate
    pub user_record_count: usize,
    /// Combined finalizers from all user records
    pub finalizers: EventFinalizers,
    /// Combined metadata from all user records
    pub metadata: RequestMetadata,
}

impl Finalizable for AggregatedRecord {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

/// KPL aggregator that packs multiple user records into aggregated records
#[derive(Clone)]
pub struct KplAggregator {
    max_records_per_aggregate: usize,
}

impl KplAggregator {
    pub fn new(max_records_per_aggregate: usize) -> Self {
        Self {
            max_records_per_aggregate,
        }
    }

    /// Returns the maximum number of records per aggregate
    pub fn max_records_per_aggregate(&self) -> usize {
        self.max_records_per_aggregate
    }

    /// Calculate the estimated final KPL record size including all overhead.
    ///
    /// This estimates the size of the final Kinesis record after KPL encoding,
    /// including magic bytes, protobuf structure, and MD5 checksum.
    fn estimate_kpl_size(&self, user_data_size: usize, record_count: usize) -> usize {
        user_data_size
            + KPL_FIXED_OVERHEAD
            + PROTOBUF_BASE_OVERHEAD
            + (PROTOBUF_PER_RECORD_OVERHEAD * record_count)
    }

    /// Aggregate a batch of user records into aggregated records
    /// Uses the first record's partition key for the entire aggregate
    pub fn aggregate_records(&self, user_records: Vec<UserRecord>) -> Vec<AggregatedRecord> {
        let total_input_records = user_records.len();
        let mut aggregated_records = Vec::new();
        let mut current_batch = VecDeque::new();
        let mut current_data_size = 0; // Total user data size (without overhead)

        for user_record in user_records.into_iter() {
            let record_data_size = user_record.data.len();

            // Handle records that would exceed Kinesis limit even as single-record aggregate
            let single_record_size = self.estimate_kpl_size(record_data_size, 1);
            if single_record_size > MAX_KINESIS_RECORD_SIZE {
                // Flush current batch if any
                if !current_batch.is_empty() {
                    if let Some(aggregated) = self.create_aggregated_record(&mut current_batch) {
                        aggregated_records.push(aggregated);
                    }
                    current_data_size = 0;
                }

                // Create single-record aggregate for the oversized record
                let mut single_record_batch = VecDeque::new();
                single_record_batch.push_back(user_record);
                if let Some(aggregated) = self.create_aggregated_record(&mut single_record_batch) {
                    tracing::info!(
                        message = "Created single-record aggregate for oversized record",
                        aggregate_data_size = aggregated.data.len(),
                        partition_key = %aggregated.partition_key,
                    );
                    aggregated_records.push(aggregated);
                }
                continue;
            }

            // Calculate what the final KPL size would be if we add this record
            let estimated_size_with_record = self.estimate_kpl_size(
                current_data_size + record_data_size,
                current_batch.len() + 1,
            );

            // Check if adding this record would exceed limits
            let would_exceed_record_count = current_batch.len() >= self.max_records_per_aggregate;
            let would_exceed_size = estimated_size_with_record > MAX_KINESIS_RECORD_SIZE;
            let should_flush = would_exceed_record_count || would_exceed_size;

            if should_flush && !current_batch.is_empty() {
                let batch_count = current_batch.len();
                let batch_data_size = current_data_size;
                let estimated_kpl_size = self.estimate_kpl_size(batch_data_size, batch_count);

                tracing::debug!(
                    message = "Flushing current batch to create aggregated record",
                    reason = if would_exceed_record_count {
                        "max_records_reached"
                    } else {
                        "size_limit_reached"
                    },
                    batch_record_count = batch_count,
                    batch_data_size = batch_data_size,
                    estimated_kpl_size = estimated_kpl_size,
                    aggregate_number = aggregated_records.len() + 1,
                );

                // Flush current batch
                if let Some(aggregated) = self.create_aggregated_record(&mut current_batch) {
                    tracing::debug!(
                        message = "Created aggregated record",
                        aggregate_index = aggregated_records.len(),
                        user_record_count = aggregated.user_record_count,
                        aggregate_data_size = aggregated.data.len(),
                        partition_key = %aggregated.partition_key,
                    );
                    aggregated_records.push(aggregated);
                }
                current_data_size = 0;
            }

            // Add record to current batch
            current_data_size += record_data_size;
            current_batch.push_back(user_record);
        }

        // Flush remaining records
        if !current_batch.is_empty() {
            let batch_count = current_batch.len();
            let batch_data_size = current_data_size;
            let estimated_kpl_size = self.estimate_kpl_size(batch_data_size, batch_count);

            tracing::debug!(
                message = "Flushing final batch",
                batch_record_count = batch_count,
                batch_data_size = batch_data_size,
                estimated_kpl_size = estimated_kpl_size,
                aggregate_number = aggregated_records.len() + 1,
            );

            if let Some(aggregated) = self.create_aggregated_record(&mut current_batch) {
                tracing::debug!(
                    message = "Created final aggregated record",
                    aggregate_index = aggregated_records.len(),
                    user_record_count = aggregated.user_record_count,
                    aggregate_data_size = aggregated.data.len(),
                    partition_key = %aggregated.partition_key,
                );
                aggregated_records.push(aggregated);
            }
        }

        let total_output_records = aggregated_records.len();
        let total_user_records: usize =
            aggregated_records.iter().map(|r| r.user_record_count).sum();
        let aggregation_ratio = if total_output_records > 0 {
            total_input_records as f64 / total_output_records as f64
        } else {
            0.0
        };

        tracing::info!(
            message = "KPL aggregation complete",
            input_records = total_input_records,
            output_aggregated_records = total_output_records,
            total_user_records_in_aggregates = total_user_records,
            aggregation_ratio = %format!("{:.2}", aggregation_ratio),
        );

        aggregated_records
    }

    /// Create an aggregated record from a batch of user records using protobuf
    fn create_aggregated_record(
        &self,
        user_records: &mut VecDeque<UserRecord>,
    ) -> Option<AggregatedRecord> {
        if user_records.is_empty() {
            tracing::trace!("Skipping empty user_records batch");
            return None;
        }

        // Build partition key table. Use a single partition key for all records in this aggregate.
        // All records in an aggregate share the same partition key, which ensures the aggregated
        // record is correctly associated with the Kinesis shard it arrives through.
        //
        // Note: We do NOT populate explicit_hash_key_table. Explicit hash keys in subrecords are
        // meaningless since shard assignment already happened at the aggregated record level.
        let shared_partition_key = crate::sinks::aws_kinesis::sink::gen_partition_key();
        let partition_key_table: Vec<String> = vec![shared_partition_key.clone()];

        // Build protobuf records
        let mut proto_records = Vec::new();

        for user_record in user_records.iter() {
            // All records use the same partition key (index 0)
            proto_records.push(kpl_proto::Record {
                partition_key_index: 0,
                explicit_hash_key_index: None,
                data: user_record.data.to_vec(),
                tags: vec![], // Tags not currently used
            });
        }

        // Create the aggregated record protobuf message
        let aggregated = kpl_proto::AggregatedRecord {
            partition_key_table: partition_key_table.clone(),
            explicit_hash_key_table: vec![],
            records: proto_records,
        };

        // Serialize the protobuf message
        let mut protobuf_data = Vec::new();
        if let Err(e) = aggregated.encode(&mut protobuf_data) {
            tracing::error!(message = "Failed to encode KPL protobuf", error = %e);
            return None;
        }

        // Build the final KPL format: [magic][protobuf][md5]

        // Calculate MD5 checksum of the protobuf data only
        let mut hasher = Md5::new();
        hasher.update(&protobuf_data);
        let checksum = hasher.finalize();

        let mut buf = BytesMut::with_capacity(KPL_MAGIC.len() + protobuf_data.len() + checksum.len());
        buf.put_slice(&KPL_MAGIC);
        buf.put_slice(&protobuf_data);
        buf.put_slice(&checksum);

        // Collect metadata and finalizers
        // Use the same partition key we already generated for the protobuf
        let partition_key = shared_partition_key;
        let user_record_count = user_records.len();

        let mut combined_finalizers = EventFinalizers::default();
        let mut metadata_builders = Vec::new();

        for mut user_record in user_records.drain(..) {
            combined_finalizers.merge(user_record.take_finalizers());
            metadata_builders.push(user_record.metadata);
        }

        let combined_metadata = RequestMetadata::from_batch(metadata_builders);

        let final_size = buf.len();
        tracing::debug!(
            message = "Finalized aggregated record",
            partition_key = %partition_key,
            user_record_count = user_record_count,
            final_data_size = final_size,
        );

        Some(AggregatedRecord {
            data: buf.freeze(),
            partition_key,
            user_record_count,
            finalizers: combined_finalizers,
            metadata: combined_metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_user_record_size_estimation() {
        let user_record = UserRecord {
            data: Bytes::from("test data"),
            partition_key: "test_key".to_string(),
            explicit_hash_key: None,
            finalizers: EventFinalizers::default(),
            metadata: RequestMetadata::default(),
        };

        // Just verify that encoded_size returns a reasonable estimate
        let size = user_record.encoded_size();
        assert!(size > 0);
        assert!(size >= user_record.data.len());
    }

    #[test]
    fn test_kpl_aggregation() {
        let aggregator = KplAggregator::new(100);

        let user_records = vec![
            UserRecord {
                data: Bytes::from("record1"),
                partition_key: "key1".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from("record2"),
                partition_key: "key1".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
        ];

        let aggregated = aggregator.aggregate_records(user_records);
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].user_record_count, 2);
        assert_eq!(aggregated[0].partition_key, "key1");

        // Verify KPL magic
        assert_eq!(&aggregated[0].data[0..4], &KPL_MAGIC);

        // Verify the format: [magic][protobuf][md5]
        let data = &aggregated[0].data;
        assert!(data.len() > 20); // At least magic + some protobuf + md5

        // Extract and verify protobuf can be decoded
        let protobuf_data = &data[4..data.len() - 16]; // Skip magic and md5
        let decoded = kpl_proto::AggregatedRecord::decode(protobuf_data);
        assert!(decoded.is_ok(), "Protobuf should decode successfully");

        let decoded = decoded.unwrap();
        assert_eq!(decoded.partition_key_table.len(), 1);
        assert_eq!(decoded.partition_key_table[0], "key1");
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].data, b"record1");
        assert_eq!(decoded.records[1].data, b"record2");

        // Verify MD5 checksum
        let mut hasher = Md5::new();
        hasher.update(protobuf_data);
        let expected_checksum = hasher.finalize();
        let actual_checksum = &data[data.len() - 16..];
        assert_eq!(actual_checksum, expected_checksum.as_slice());
    }

    #[test]
    fn test_partition_key_table_deduplication() {
        let aggregator = KplAggregator::new(100);

        let user_records = vec![
            UserRecord {
                data: Bytes::from("record1"),
                partition_key: "key1".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from("record2"),
                partition_key: "key2".to_string(), // Different partition key
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from("record3"),
                partition_key: "key1".to_string(), // Same as first - should be deduplicated
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
        ];

        let aggregated = aggregator.aggregate_records(user_records);
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].user_record_count, 3);
        assert_eq!(aggregated[0].partition_key, "key1"); // Uses first record's key

        // Verify protobuf has deduplicated partition keys
        let data = &aggregated[0].data;
        let protobuf_data = &data[4..data.len() - 16];
        let decoded = kpl_proto::AggregatedRecord::decode(protobuf_data).unwrap();

        // Should only have 2 unique partition keys in the table
        assert_eq!(decoded.partition_key_table.len(), 2);
        assert!(decoded.partition_key_table.contains(&"key1".to_string()));
        assert!(decoded.partition_key_table.contains(&"key2".to_string()));

        // Verify records reference the correct indices
        assert_eq!(decoded.records.len(), 3);
        // First and third records should reference the same partition key index
        assert_eq!(
            decoded.records[0].partition_key_index,
            decoded.records[2].partition_key_index
        );
    }

    #[test]
    fn test_oversized_record_handling() {
        let aggregator = KplAggregator::new(100);

        // Create a record that's too large (> 950KB limit)
        let large_data = vec![0u8; 1_000_000]; // 1MB data (exceeds 950KB limit)
        let user_records = vec![
            UserRecord {
                data: Bytes::from("normal record"),
                partition_key: "key1".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from(large_data), // This record is too large
                partition_key: "key2".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from("another normal record"),
                partition_key: "key3".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
        ];

        let aggregated = aggregator.aggregate_records(user_records);

        // Should create three aggregates:
        // 1. First normal record
        // 2. Oversized record as single-record aggregate
        // 3. Second normal record
        assert_eq!(aggregated.len(), 3);

        // First aggregate: normal record
        assert_eq!(aggregated[0].user_record_count, 1);

        // Second aggregate: oversized record sent as single-record aggregate
        assert_eq!(aggregated[1].user_record_count, 1);
        assert!(aggregated[1].data.len() > 1_000_000); // Large aggregate

        // Third aggregate: another normal record
        assert_eq!(aggregated[2].user_record_count, 1);
    }

    #[test]
    fn test_shared_partition_key_in_aggregate() {
        let aggregator = KplAggregator::new(100);

        // Create records with various partition keys (Unicode, ASCII, etc.)
        // The aggregated record will use a single generated key for all records
        let user_records = vec![
            UserRecord {
                data: Bytes::from("record1"),
                partition_key: "\u{109ea2}\u{0c69dd}\u{076aec}".to_string(), // Unicode
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
            UserRecord {
                data: Bytes::from("record2"),
                partition_key: "normalASCIIkey123".to_string(), // ASCII
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
        ];

        let aggregated = aggregator.aggregate_records(user_records);
        assert_eq!(aggregated.len(), 1);

        // The aggregated record should have a partition key (16 random characters)
        let partition_key = &aggregated[0].partition_key;
        assert_eq!(
            partition_key.len(),
            16,
            "Partition key should be 16 characters, got: {}",
            partition_key.len()
        );

        // Verify the protobuf has only ONE partition key (shared by all records)
        let data = &aggregated[0].data;
        let protobuf_data = &data[4..data.len() - 16];
        let decoded = kpl_proto::AggregatedRecord::decode(protobuf_data).unwrap();

        // Since we now use a single generated key for all records in the aggregate,
        // the partition key table should only have 1 entry
        assert_eq!(
            decoded.partition_key_table.len(),
            1,
            "Partition key table should have exactly 1 entry (all records use same key)"
        );

        // All records should reference the same partition key index (0)
        assert!(
            decoded.records.iter().all(|r| r.partition_key_index == 0),
            "All records should reference partition key index 0"
        );
    }
}
