//! Kafka payloads: MessagePack with string field names (`to_vec_named`).

use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode_msgpack_named<T: Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::encode::to_vec_named(value).unwrap_or_else(|e| {
        panic!("kafka msgpack encode failed: {}", e);
    })
}

pub fn decode_msgpack<T: DeserializeOwned>(data: &[u8]) -> Result<T, String> {
    rmp_serde::from_slice(data).map_err(|e| format!("kafka msgpack decode failed: {}", e))
}
