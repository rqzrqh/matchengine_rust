//! Kafka publish payloads: MessagePack with string field names (`to_vec_named`).

use serde::Serialize;

pub fn encode_msgpack_named<T: Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::encode::to_vec_named(value).unwrap_or_else(|e| {
        panic!("kafka msgpack encode failed: {}", e);
    })
}
