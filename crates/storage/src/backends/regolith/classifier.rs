//! The store's key classes, declared to regolith's commit validator.
//!
//! The class of every family is declared once, in `families`: blocks are
//! content addressed, the arrival journal is a log, a document's or a
//! collection's head set (its heads, or the markers against them) is a
//! commutative prefix, and every other key is ordinary. A commutative prefix
//! is one head set, never a whole family: a scan inside one document's heads
//! chooses parents and decides nothing else, while a scan across documents
//! is validated as any scan is. A key a family does not recognise, or one cut
//! short or malformed, stays ordinary.

use regolith::{KeyClass, KeyClassifier};

use super::families::{self, Class};

pub(super) struct DefraKeyClassifier;

impl KeyClassifier for DefraKeyClassifier {
    fn classify(&self, key: &[u8]) -> KeyClass {
        let Some(family) = families::family_of(key) else {
            return KeyClass::Ordinary;
        };
        match family.class() {
            Class::Ordinary => KeyClass::Ordinary,
            Class::CommutativeSet => family
                .entity_len(key)
                .map_or(KeyClass::Ordinary, |len| KeyClass::CommutativePrefix { len }),
            // CID parsing uses fixed-size multihash storage and allocates
            // nothing.
            Class::ContentAddressed if cid::Cid::try_from(family.rest(key)).is_ok() => {
                KeyClass::ContentAddressed
            }
            Class::ContentAddressed => KeyClass::Ordinary,
            Class::Log => KeyClass::Log,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
