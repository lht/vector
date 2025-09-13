use std::collections::VecDeque;
use bytes::{Bytes, BytesMut, BufMut};
use md5::{Md5, Digest};
use crate::event::{EventFinalizers, Finalizable};
use vector_lib::{request_metadata::RequestMetadata, ByteSizeOf};

// KPL Magic bytes - identifies this as a KPL aggregated record
const KPL_MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];
const MAX_AGGREGATE_SIZE: usize = 900_000; // 900KB to stay under 1MB limit

/// Individual user record before aggregation
#[derive(Debug, Clone)]
pub struct UserRecord {
    /// Compressed data payload
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
    /// Calculate the encoded size of this record in KPL format
    pub fn encoded_size(&self) -> usize {
        4 + // data length (u32)
        self.data.len() + // data
        1 + // partition key length (u8)
        self.partition_key.len() + // partition key
        1 + // explicit hash key length (u8)
        self.explicit_hash_key.as_ref().map_or(0, |k| k.len()) // explicit hash key
    }
    
    /// Encode this record into the KPL format
    pub fn encode(&self, buf: &mut BytesMut) {
        // Data length + data
        buf.put_u32(self.data.len() as u32);
        buf.put_slice(&self.data);
        
        // Partition key length + partition key
        let pk_len = std::cmp::min(self.partition_key.len(), 255);
        buf.put_u8(pk_len as u8);
        buf.put_slice(&self.partition_key.as_bytes()[..pk_len]);
        
        // Explicit hash key length + explicit hash key
        if let Some(ref hash_key) = self.explicit_hash_key {
            let hk_len = std::cmp::min(hash_key.len(), 255);
            buf.put_u8(hk_len as u8);
            buf.put_slice(&hash_key.as_bytes()[..hk_len]);
        } else {
            buf.put_u8(0);
        }
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
        self.data.len() + self.partition_key.len() + 
        self.explicit_hash_key.as_ref().map_or(0, |k| k.len())
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
    
    /// Aggregate a batch of user records into aggregated records
    pub fn aggregate_records(&self, user_records: Vec<UserRecord>) -> Vec<AggregatedRecord> {
        let mut aggregated_records = Vec::new();
        let mut current_batch = VecDeque::new();
        let mut current_size = 0;
        let mut current_partition_key: Option<String> = None;
        
        for user_record in user_records {
            let record_size = user_record.encoded_size();
            let partition_key = &user_record.partition_key;
            
            // Check if we should start a new aggregate
            let should_flush = current_batch.len() >= self.max_records_per_aggregate
                || current_size + record_size > self.max_aggregate_size
                || (current_partition_key.is_some() 
                    && current_partition_key.as_ref() != Some(partition_key))
                || current_batch.is_empty();
            
            if should_flush && !current_batch.is_empty() {
                // Flush current batch
                if let Some(aggregated) = self.create_aggregated_record(&mut current_batch) {
                    aggregated_records.push(aggregated);
                }
                current_size = 0;
                current_partition_key = None;
            }
            
            // Add record to current batch
            if current_partition_key.is_none() {
                current_partition_key = Some(partition_key.clone());
            }
            current_size += record_size;
            current_batch.push_back(user_record);
        }
        
        // Flush remaining records
        if !current_batch.is_empty() {
            if let Some(aggregated) = self.create_aggregated_record(&mut current_batch) {
                aggregated_records.push(aggregated);
            }
        }
        
        aggregated_records
    }
    
    /// Create an aggregated record from a batch of user records
    fn create_aggregated_record(&self, user_records: &mut VecDeque<UserRecord>) -> Option<AggregatedRecord> {
        if user_records.is_empty() {
            return None;
        }
        
        // Estimate buffer size
        let estimated_size = user_records.iter()
            .map(|r| r.encoded_size())
            .sum::<usize>() + 24; // magic + count + md5
        
        let mut buf = BytesMut::with_capacity(estimated_size);
        
        // Write KPL magic
        buf.put_slice(&KPL_MAGIC);
        
        // Write record count
        buf.put_u32(user_records.len() as u32);
        
        // Write all user records
        for user_record in user_records.iter() {
            user_record.encode(&mut buf);
        }
        
        // Calculate and write MD5 checksum
        let mut hasher = Md5::new();
        hasher.update(&buf[8..]); // Skip magic and count for checksum
        let checksum = hasher.finalize();
        buf.put_slice(&checksum);
        
        // Collect metadata and finalizers
        let partition_key = user_records.front()?.partition_key.clone();
        let user_record_count = user_records.len();
        
        let mut combined_finalizers = EventFinalizers::default();
        let mut metadata_builders = Vec::new();
        
        for mut user_record in user_records.drain(..) {
            combined_finalizers.merge(user_record.take_finalizers());
            metadata_builders.push(user_record.metadata);
        }
        
        let combined_metadata = RequestMetadata::from_batch(metadata_builders);
        
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
    fn test_user_record_encoding() {
        let user_record = UserRecord {
            data: Bytes::from("test data"),
            partition_key: "test_key".to_string(),
            explicit_hash_key: None,
            finalizers: EventFinalizers::default(),
            metadata: RequestMetadata::default(),
        };
        
        let mut buf = BytesMut::new();
        user_record.encode(&mut buf);
        
        // Verify the encoding format
        assert_eq!(buf.len(), 4 + 9 + 1 + 8 + 1); // data_len + data + pk_len + pk + hk_len
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
    }
    
    #[test]
    fn test_partition_key_separation() {
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
                partition_key: "key2".to_string(),
                explicit_hash_key: None,
                finalizers: EventFinalizers::default(),
                metadata: RequestMetadata::default(),
            },
        ];
        
        let aggregated = aggregator.aggregate_records(user_records);
        // Should create separate aggregates for different partition keys
        assert_eq!(aggregated.len(), 2);
        assert_eq!(aggregated[0].user_record_count, 1);
        assert_eq!(aggregated[1].user_record_count, 1);
    }
}