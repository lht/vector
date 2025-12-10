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
//! | PKey: "A"    |      | PKey: "A"    |      | PKey: "B"    |      | PKey: "B"    |
//! | EHKey: null  |      | EHKey: "X"   |      | EHKey: null  |      | EHKey: "Y"   |
//! +--------------+      +--------------+      +--------------+      +--------------+
//!        |                     |                     |                     |
//!        +----------+----------+                     +----------+----------+
//!                   |                                           |
//!                   v                                           v
//! +-------------------------------------------------------------------------------+
//! |                        AGGREGATED RECORDS (KPL Format)                        |
//! +-------------------------------------------------------------------------------+
//!
//!     Aggregated Record 1                          Aggregated Record 2
//! +----------------------------+              +----------------------------+
//! | +--------+                 |              | +--------+                 |
//! | | Magic  | (4 bytes)       |              | | Magic  | (4 bytes)       |
//! | |0xF3899AC2                |              | |0xF3899AC2                |
//! | +--------+                 |              | +--------+                 |
//! | +----------------------+   |              | +----------------------+   |
//! | |   Protobuf Message   |   |              | |   Protobuf Message   |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | | |partition_key     | |   |              | | |partition_key     | |   |
//! | | |  _table ["A"]    | |   |              | | |  _table ["B"]    | |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | | |explicit_hash_key | |   |              | | |explicit_hash_key | |   |
//! | | |  _table ["X"]    | |   |              | | |  _table ["Y"]    | |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | | | records[]        | |   |              | | | records[]        | |   |
//! | | |  Record 1:       | |   |              | | |  Record 3:       | |   |
//! | | |   pk_index: 0    | |   |              | | |   pk_index: 0    | |   |
//! | | |   ehk_index: -   | |   |              | | |   ehk_index: -   | |   |
//! | | |   data: "..."    | |   |              | | |   data: "..."    | |   |
//! | | |  Record 2:       | |   |              | | |  Record 4:       | |   |
//! | | |   pk_index: 0    | |   |              | | |   pk_index: 0    | |   |
//! | | |   ehk_index: 0   | |   |              | | |   ehk_index: 0   | |   |
//! | | |   data: "..."    | |   |              | | |   data: "..."    | |   |
//! | | +------------------+ |   |              | | +------------------+ |   |
//! | +----------------------+   |              | +----------------------+   |
//! | +--------+                 |              | +--------+                 |
//! | |MD5 Sum | (16 bytes)      |              | |MD5 Sum | (16 bytes)      |
//! | |of Proto|                 |              | |of Proto|                 |
//! | +--------+                 |              | +--------+                 |
//! |                            |              |                            |
//! | Partition Key: "A"         |              | Partition Key: "B"         |
//! | User Record Count: 2       |              | User Record Count: 2       |
//! | Size: ~950KB max           |              | Size: ~950KB max           |
//! +----------------------------+              +----------------------------+
//!        |                                            |
//!        +------------------+--------------------------+
//!                           |
//!                           v
//! +-------------------------------------------------------------------------------+
//! |                    BATCHING FOR PUTRECORDS API CALL                           |
//! +-------------------------------------------------------------------------------+
//!
//! PutRecords API Request (max 500 records or 5MB per request)
//! +------------------------------------------------------------------------------+
//! | StreamName: "my-stream"                                                      |
//! | Records: [                                                                   |
//! |   {                                                                          |
//! |     Data: <Aggregated Record 1 binary>  // Contains 2 user records          |
//! |     PartitionKey: "A"                                                        |
//! |   },                                                                         |
//! |   {                                                                          |
//! |     Data: <Aggregated Record 2 binary>  // Contains 2 user records          |
//! |     PartitionKey: "B"                                                        |
//! |   },                                                                         |
//! |   ... (up to 500 aggregated records)                                         |
//! | ]                                                                            |
//! +------------------------------------------------------------------------------+
//!                           |
//!                           v
//! +-------------------------------------------------------------------------------+
//! |                        AWS KINESIS DATA STREAM                                |
//! |                                                                               |
//! |  Shard 1                      Shard 2                      Shard N           |
//! | +---------+                  +---------+                  +---------+        |
//! | | Agg 1   |                  | Agg 2   |                  | ...     |        |
//! | | (2 user)|                  | (2 user)|                  |         |        |
//! | +---------+                  +---------+                  +---------+        |
//! +-------------------------------------------------------------------------------+
//! ```
//!
//! ## Benefits of KPL Aggregation
//!
//! - **Improved Throughput**: Pack multiple small records into fewer Kinesis records
//! - **Cost Reduction**: Pay for fewer PUT operations (Kinesis charges per record)
//! - **Better Shard Utilization**: More efficient use of shard write capacity (1MB/sec or 1000 records/sec)
//! - **Deduplication**: Partition keys and explicit hash keys are stored once per aggregate
//!
//! ## Format Details
//!
//! ### Aggregated Record Structure
//! ```text
//! [ 4 bytes Magic ] [ Variable Protobuf Data ] [ 16 bytes MD5 ]
//! ```
//!
//! - **Magic**: `0xF3 0x89 0x9A 0xC2` - Identifies the record as KPL aggregated
//! - **Protobuf Data**: Protocol Buffer encoded `AggregatedRecord` message
//! - **MD5 Checksum**: MD5 hash of the protobuf data for integrity verification
//!
//! ### Protobuf Schema (simplified)
//! ```protobuf
//! message AggregatedRecord {
//!   repeated string partition_key_table = 1;        // Deduplicated partition keys
//!   repeated string explicit_hash_key_table = 2;    // Deduplicated explicit hash keys
//!   repeated Record records = 3;                    // User records with indices
//! }
//!
//! message Record {
//!   uint64 partition_key_index = 1;                 // Index into partition_key_table
//!   optional uint64 explicit_hash_key_index = 2;    // Index into explicit_hash_key_table
//!   bytes data = 3;                                 // User record data
//!   repeated Tag tags = 4;                          // Optional tags
//! }
//! ```
//!
//! ## Size Limits
//!
//! - **Max Aggregated Record Size**: 950KB (leaves room for overhead under 1MB AWS limit)
//! - **Max Records per Aggregate**: Configurable (default varies by implementation)
//! - **Max PutRecords Batch**: 500 records or 5MB total per API call
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
use std::collections::{HashMap, VecDeque};
use vector_lib::{ByteSizeOf, request_metadata::RequestMetadata};

// Include the generated protobuf code
pub mod kpl_proto {
    include!(concat!(env!("OUT_DIR"), "/kpl_aggregation.rs"));
}

// KPL Magic bytes - identifies this as a KPL aggregated record
const KPL_MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];
const MAX_AGGREGATE_SIZE: usize = 950_000; // 950KB binary data + overhead < 1MB AWS limit

/// Generate a safe ASCII-only partition key for aggregated records.
/// Uses alphanumeric characters to ensure compatibility with Java-based KCL consumers.
fn generate_safe_partition_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::rng();
    (0..16)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

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
        // Approximate protobuf overhead: field tags, lengths, etc.
        // partition_key_index field (varint) + data field + tags
        self.data.len()
            + self.partition_key.len()
            + self.explicit_hash_key.as_ref().map_or(0, |k| k.len())
            + 20 // overhead estimate
    }
}

impl Finalizable for UserRecord {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl ByteSizeOf for UserRecord {
    fn size_of(&self) -> usize {
        self.encoded_size()
    }

    fn allocated_bytes(&self) -> usize {
        self.data.len()
            + self.partition_key.len()
            + self.explicit_hash_key.as_ref().map_or(0, |k| k.len())
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
    max_aggregate_size: usize,
}

impl KplAggregator {
    pub fn new(max_records_per_aggregate: usize) -> Self {
        Self {
            max_records_per_aggregate,
            max_aggregate_size: MAX_AGGREGATE_SIZE,
        }
    }

    /// Returns the maximum number of records per aggregate
    pub fn max_records_per_aggregate(&self) -> usize {
        self.max_records_per_aggregate
    }

    /// Aggregate a batch of user records into aggregated records
    /// Uses the first record's partition key for the entire aggregate
    pub fn aggregate_records(&self, user_records: Vec<UserRecord>) -> Vec<AggregatedRecord> {
        let total_input_records = user_records.len();
        tracing::debug!(
            message = "Starting KPL aggregation",
            input_records = total_input_records,
            max_records_per_aggregate = self.max_records_per_aggregate,
            max_aggregate_size = self.max_aggregate_size,
        );

        let mut aggregated_records = Vec::new();
        let mut current_batch = VecDeque::new();
        let mut current_size = 0;
        let mut skipped_oversized = 0;

        for (idx, user_record) in user_records.into_iter().enumerate() {
            let record_size = user_record.encoded_size();

            tracing::trace!(
                message = "Processing user record",
                record_index = idx,
                record_size = record_size,
                partition_key = %user_record.partition_key,
                explicit_hash_key = ?user_record.explicit_hash_key,
                current_batch_size = current_batch.len(),
                current_size_bytes = current_size,
            );

            // Safety check: Skip records that are too large for any aggregate
            if record_size > self.max_aggregate_size {
                skipped_oversized += 1;
                tracing::warn!(
                    message = "Skipping oversized user record",
                    record_index = idx,
                    record_size = record_size,
                    max_aggregate_size = self.max_aggregate_size,
                    partition_key = %user_record.partition_key,
                );
                continue;
            }

            // Check if we should start a new aggregate
            let should_flush = current_batch.len() >= self.max_records_per_aggregate
                || current_size + record_size > self.max_aggregate_size;

            if should_flush && !current_batch.is_empty() {
                let batch_count = current_batch.len();
                let batch_size = current_size;

                tracing::debug!(
                    message = "Flushing current batch to create aggregated record",
                    reason = if current_batch.len() >= self.max_records_per_aggregate {
                        "max_records_reached"
                    } else {
                        "size_limit_reached"
                    },
                    batch_record_count = batch_count,
                    batch_size_bytes = batch_size,
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
                current_size = 0;
            }

            // Add record to current batch
            current_size += record_size;
            current_batch.push_back(user_record);
        }

        // Flush remaining records
        if !current_batch.is_empty() {
            let batch_count = current_batch.len();
            let batch_size = current_size;

            tracing::debug!(
                message = "Flushing final batch",
                batch_record_count = batch_count,
                batch_size_bytes = batch_size,
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
            skipped_oversized_records = skipped_oversized,
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

        let record_count = user_records.len();
        tracing::trace!(
            message = "Building KPL protobuf structure",
            user_record_count = record_count,
        );

        // Build partition key and explicit hash key tables
        // Use a single generated ASCII partition key for all records in this aggregate.
        // This matches the Golang KPL implementation and avoids Java UTF-16 issues.
        let shared_partition_key = generate_safe_partition_key();
        let partition_key_table: Vec<String> = vec![shared_partition_key.clone()];
        let mut explicit_hash_key_table: Vec<String> = Vec::new();
        let mut explicit_hash_key_indices: HashMap<String, u64> = HashMap::new();

        tracing::trace!(
            message = "Using shared partition key for all records in aggregate",
            partition_key = %shared_partition_key,
            record_count = user_records.len(),
        );

        // Build protobuf records
        let mut proto_records = Vec::new();

        for (idx, user_record) in user_records.iter().enumerate() {
            // All records use the same partition key (index 0)
            let pk_index = 0u64;

            // Get or insert explicit hash key (if present)
            let ehk_index = if let Some(ref ehk) = user_record.explicit_hash_key {
                let idx = if let Some(&idx) = explicit_hash_key_indices.get(ehk) {
                    idx
                } else {
                    let idx = explicit_hash_key_table.len() as u64;
                    explicit_hash_key_table.push(ehk.clone());
                    explicit_hash_key_indices.insert(ehk.clone(), idx);
                    tracing::trace!(
                        message = "Added new explicit hash key to table",
                        explicit_hash_key = %ehk,
                        table_index = idx,
                    );
                    idx
                };
                Some(idx)
            } else {
                None
            };

            tracing::trace!(
                message = "Adding user record to protobuf",
                proto_record_index = idx,
                partition_key_index = pk_index,
                explicit_hash_key_index = ?ehk_index,
                data_size = user_record.data.len(),
            );

            proto_records.push(kpl_proto::Record {
                partition_key_index: pk_index,
                explicit_hash_key_index: ehk_index,
                data: user_record.data.to_vec(),
                tags: vec![], // Tags not currently used
            });
        }

        tracing::debug!(
            message = "Built KPL protobuf structure",
            partition_key_table_size = partition_key_table.len(),
            explicit_hash_key_table_size = explicit_hash_key_table.len(),
            proto_records_count = proto_records.len(),
        );

        // Create the aggregated record protobuf message
        let aggregated = kpl_proto::AggregatedRecord {
            partition_key_table: partition_key_table.clone(),
            explicit_hash_key_table: explicit_hash_key_table.clone(),
            records: proto_records,
        };

        // Serialize the protobuf message
        let mut protobuf_data = Vec::new();
        if let Err(e) = aggregated.encode(&mut protobuf_data) {
            tracing::error!(
                message = "Failed to encode KPL protobuf",
                error = %e,
            );
            return None;
        }

        let protobuf_size = protobuf_data.len();
        tracing::trace!(
            message = "Encoded protobuf data",
            protobuf_size = protobuf_size,
        );

        // Build the final KPL format: [magic][protobuf][md5]
        let mut buf = BytesMut::with_capacity(4 + protobuf_data.len() + 16);

        // Write KPL magic
        buf.put_slice(&KPL_MAGIC);
        tracing::trace!(
            message = "Added KPL magic bytes",
            magic_hex = format!(
                "{:02x}{:02x}{:02x}{:02x}",
                KPL_MAGIC[0], KPL_MAGIC[1], KPL_MAGIC[2], KPL_MAGIC[3]
            ),
        );

        // Write protobuf data
        buf.put_slice(&protobuf_data);

        // Calculate and write MD5 checksum of the protobuf data only
        let mut hasher = Md5::new();
        hasher.update(&protobuf_data);
        let checksum = hasher.finalize();
        buf.put_slice(&checksum);

        tracing::trace!(
            message = "Added MD5 checksum",
            checksum_hex = format!("{:x}", checksum),
        );

        let final_size = buf.len();
        tracing::debug!(
            message = "Built final KPL aggregated record",
            total_size = final_size,
            magic_size = 4,
            protobuf_size = protobuf_size,
            checksum_size = 16,
        );

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
    fn test_oversized_record_safety_check() {
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

        // Should create one aggregate with only the normal records (large record skipped)
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].user_record_count, 2); // Only 2 records (large one skipped)

        // Verify the aggregated record is under the size limit
        assert!(aggregated[0].data.len() < 1_000_000); // Under 1MB
    }

    #[test]
    fn test_aggregated_partition_key_is_ascii() {
        let aggregator = KplAggregator::new(100);

        // Create records with various partition keys (Unicode, ASCII, etc.)
        // The aggregated record should always use a generated ASCII key
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

        // The aggregated record's partition key should be ASCII alphanumeric
        let partition_key = &aggregated[0].partition_key;

        // Should be ASCII-only
        assert!(partition_key.chars().all(|c| c.is_ascii()),
            "Partition key should be ASCII-only, got: {}", partition_key);

        // Should be alphanumeric
        assert!(partition_key.chars().all(|c| c.is_ascii_alphanumeric()),
            "Partition key should be alphanumeric, got: {}", partition_key);

        // Should be 16 characters
        assert_eq!(partition_key.len(), 16,
            "Partition key should be 16 characters, got: {}", partition_key.len());

        // Verify the protobuf has deduplicated partition keys
        let data = &aggregated[0].data;
        let protobuf_data = &data[4..data.len() - 16];
        let decoded = kpl_proto::AggregatedRecord::decode(protobuf_data).unwrap();

        // Since we now use a single generated key for all records in the aggregate,
        // the partition key table should only have 1 entry
        assert_eq!(decoded.partition_key_table.len(), 1,
            "Partition key table should have exactly 1 entry (all records use same key)");

        // All records should reference the same partition key index
        assert!(decoded.records.iter().all(|r| r.partition_key_index == 0),
            "All records should reference partition key index 0");
    }
}
