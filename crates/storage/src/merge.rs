//! Versioned merge operands owned by Defra. Base values retain their typed
//! big-endian representation; operands are never exposed as materialized values.
use regolith::MergeOperator;

const MAGIC: &[u8] = b"DM\x01";
const INT64: u8 = 1;
const FLOAT32: u8 = 2;
const FLOAT64: u8 = 3;
const STATS: u8 = 4;

fn encode(kind: u8, value: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(4 + value.len());
    result.extend_from_slice(MAGIC);
    result.push(kind);
    result.extend_from_slice(value);
    result
}

fn decode(bytes: &[u8]) -> Option<(u8, &[u8])> {
    let bytes = bytes.strip_prefix(MAGIC)?;
    Some((*bytes.first()?, &bytes[1..]))
}

pub fn counter_i64(delta: i64) -> Vec<u8> {
    encode(INT64, &delta.to_be_bytes())
}
pub fn counter_f32(delta: f32) -> Vec<u8> {
    encode(FLOAT32, &delta.to_be_bytes())
}
pub fn counter_f64(delta: f64) -> Vec<u8> {
    encode(FLOAT64, &delta.to_be_bytes())
}
pub fn stats_delta(documents: i128, field_length: i128) -> Vec<u8> {
    encode(
        STATS,
        &[documents.to_be_bytes(), field_length.to_be_bytes()].concat(),
    )
}

pub struct DefraMergeOperator;

impl MergeOperator for DefraMergeOperator {
    fn name(&self) -> &'static str {
        "defra-values-v1"
    }

    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>> {
        let Some(first) = operands.first() else {
            return base.map(<[u8]>::to_vec);
        };
        let (kind, _) = decode(first)?;
        let mut value = base.map(<[u8]>::to_vec).unwrap_or_else(|| {
            vec![
                0;
                match kind {
                    INT64 | FLOAT64 => 8,
                    FLOAT32 => 4,
                    STATS => 32,
                    _ => 0,
                }
            ]
        });
        for operand in operands {
            let (next_kind, delta) = decode(operand)?;
            if next_kind != kind {
                return None;
            }
            value = match kind {
                INT64 => i64::from_be_bytes(value.as_slice().try_into().ok()?)
                    .wrapping_add(i64::from_be_bytes(delta.try_into().ok()?))
                    .to_be_bytes()
                    .to_vec(),
                FLOAT32 => (f32::from_be_bytes(value.as_slice().try_into().ok()?)
                    + f32::from_be_bytes(delta.try_into().ok()?))
                .to_be_bytes()
                .to_vec(),
                FLOAT64 => (f64::from_be_bytes(value.as_slice().try_into().ok()?)
                    + f64::from_be_bytes(delta.try_into().ok()?))
                .to_be_bytes()
                .to_vec(),
                STATS => {
                    if value.len() != 32 || delta.len() != 32 {
                        return None;
                    }
                    let documents = i128::from_be_bytes(value[..16].try_into().ok()?)
                        .checked_add(i128::from_be_bytes(delta[..16].try_into().ok()?))?;
                    let length = i128::from_be_bytes(value[16..].try_into().ok()?)
                        .checked_add(i128::from_be_bytes(delta[16..].try_into().ok()?))?;
                    [documents.to_be_bytes(), length.to_be_bytes()].concat()
                }
                _ => return None,
            };
        }
        Some(value)
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
        let (INT64, left) = decode(left)? else {
            return None;
        };
        let (INT64, right) = decode(right)? else {
            return None;
        };
        Some(counter_i64(
            i64::from_be_bytes(left.try_into().ok()?)
                .wrapping_add(i64::from_be_bytes(right.try_into().ok()?)),
        ))
    }
    // Floats and checked i128 stats retain their operand order. Folding a pair
    // could change rounding or hide an overflow against a later-supplied base.
}
