//! Defra key semantics declared to Regolith's commit validator.

use regolith::{KeyClass, KeyClassifier};

pub(super) struct DefraKeyClassifier;

impl KeyClassifier for DefraKeyClassifier {
    fn classify(&self, key: &[u8]) -> KeyClass {
        match key {
            // BlockStore adds `b` to raw CID bytes. `bm` stores mutable
            // merge markers, so it must never inherit block semantics. CID parsing
            // uses fixed-size multihash storage and performs no allocation.
            [b'b', cid @ ..] if cid::Cid::try_from(cid).is_ok() => KeyClass::ContentAddressed,
            // This family is written exclusively through commit-ordered append.
            key if key.starts_with(b"s/arrival/") => KeyClass::Log,
            // Immutable head entries and per-parent/child markers. Scans only
            // choose DAG parents; ordinary decision keys remain validated.
            key if key.starts_with(b"h/d/") || key.starts_with(b"h/c/") => {
                KeyClass::CommutativePrefix { len: 4 }
            }
            key if key.starts_with(b"h/ds/") || key.starts_with(b"h/cs/") => {
                KeyClass::CommutativePrefix { len: 5 }
            }
            // ACP, definitions, uniqueness and exactly-once guards stay ordinary.
            _ => KeyClass::Ordinary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_families_are_exact_commutative_prefixes() {
        for prefix in [b"h/d/".as_slice(), b"h/c/", b"h/ds/", b"h/cs/"] {
            for suffix in [b"".as_slice(), b"a/b", b"\xff\0"] {
                assert_eq!(
                    DefraKeyClassifier.classify(&[prefix, suffix].concat()),
                    KeyClass::CommutativePrefix { len: prefix.len() }
                );
            }
        }
        for key in [b"h/d".as_slice(), b"h/different/", b"h/cs", b"h/p/applied"] {
            assert_eq!(DefraKeyClassifier.classify(key), KeyClass::Ordinary);
        }
    }

    #[test]
    fn only_block_values_are_content_addressed() {
        assert_eq!(
            DefraKeyClassifier.classify(b"b\x01\x55\x00\x03abc"),
            KeyClass::ContentAddressed
        );
        for key in [
            b"".as_slice(),
            b"b",
            b"bm\x01\x55\x00\x03abc",
            b"h/p/applied",
            b"h/r/published",
            b"s/d/p/doc",
            b"a/zanzibar/owner",
            b"d/del/doc",
            b"d/data/doc",
            b"unknown",
            b"balance",
            b"b\x01invalid",
        ] {
            assert_eq!(
                DefraKeyClassifier.classify(key),
                KeyClass::Ordinary,
                "{key:?}"
            );
        }
    }
}
