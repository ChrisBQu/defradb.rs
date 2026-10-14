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
            // Head sets still eagerly delete entries, and the arrival journal
            // still uses puts. Relax these only with their new write protocols.
            // ACP, definitions, uniqueness and exactly-once guards stay ordinary.
            _ => KeyClass::Ordinary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            b"h/d/doc/head",
            b"h/c/collection/head",
            b"h/p/applied",
            b"s/arrival/collection/head",
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
