use std::io;

use bytes::Bytes;

use super::{encoding::Encoder, Compression, Compressor};

pub struct EncodeResult<P> {
    pub payload: P,
    pub uncompressed_byte_size: usize,
    pub compressed_byte_size: Option<usize>,
}

impl<P> EncodeResult<P>
where
    P: AsRef<[u8]>,
{
    pub fn uncompressed(payload: P) -> Self {
        let uncompressed_byte_size = payload.as_ref().len();
        Self {
            payload,
            uncompressed_byte_size,
            compressed_byte_size: None,
        }
    }

    pub fn compressed(payload: P, uncompressed_byte_size: usize) -> Self {
        let compressed_byte_size = payload.as_ref().len();
        Self {
            payload,
            uncompressed_byte_size,
            compressed_byte_size: Some(compressed_byte_size),
        }
    }
}

impl<P> EncodeResult<P> {
    // Can't be `const` because you can't (yet?) run deconstructors in a const context, which is what this function does
    // by dropping the (un)compressed sizes.
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_payload(self) -> P {
        self.payload
    }
}

/// Generalized interface for defining how a batch of events will be turned into a request.
pub trait RequestBuilder<Input> {
    type Metadata;
    type Events;
    type Encoder: Encoder<Self::Events>;
    type Payload: From<Bytes> + AsRef<[u8]>;
    type Request;
    type Error: From<io::Error>;

    /// Gets the compression algorithm used by this request builder.
    fn compression(&self) -> Compression;

    /// Gets the encoder used by this request builder.
    fn encoder(&self) -> &Self::Encoder;

    /// Splits apart the input into the metadata and event portions.
    ///
    /// The metadata should be any information that needs to be passed back to `build_request`
    /// as-is, such as event finalizers, while the events are the actual events to process.
    fn split_input(&self, input: Input) -> (Self::Metadata, Self::Events);

    fn encode_events(
        &self,
        events: Self::Events,
    ) -> Result<EncodeResult<Self::Payload>, Self::Error> {
        let mut compressor = Compressor::from(self.compression());
        let is_compressed = compressor.is_compressed();
        let _ = self.encoder().encode_input(events, &mut compressor)?;

        let payload = compressor.into_inner().freeze();
        let result = if is_compressed {
            let compressed_byte_size = payload.len();
            EncodeResult::compressed(payload.into(), compressed_byte_size)
        } else {
            EncodeResult::uncompressed(payload.into())
        };

        Ok(result)
    }

    /// Builds a request for the given metadata and payload.
    fn build_request(
        &self,
        metadata: Self::Metadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request;
}

/// Generalized interface for defining how a batch of events will incrementally be turned into requests.
///
/// As opposed to `RequestBuilder`, this trait provides the means to incrementally build requests
/// from a single batch of events, where all events in the batch may not fit into a single request.
/// This can be important for sinks where the underlying service has limitations on the size of a
/// request, or how many events may be present, necessitating a batch be split up into multiple requests.
///
/// While batches can be limited in size before being handed off to a request builder, we can't
/// always know in advance how large the encoded payload will be, which requires us to be able to
/// potentially split a batch into multiple requests.
pub trait IncrementalRequestBuilder<Input> {
    type Metadata;
    type Payload;
    type Request;
    type Error;

    /// Incrementally encodes the given input, potentially generating multiple payloads.
    fn encode_events_incremental(
        &mut self,
        input: Input,
    ) -> Vec<Result<(Self::Metadata, Self::Payload), Self::Error>>;

    /// Builds a request for the given metadata and payload.
    fn build_request(&mut self, metadata: Self::Metadata, payload: Self::Payload) -> Self::Request;
}
