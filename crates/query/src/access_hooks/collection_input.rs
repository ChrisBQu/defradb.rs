//! Preserve runtime schema fields in exact validator inputs without changing
//! the persisted CollectionVersion representation.
use schema::CollectionVersion;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(super) fn serialize<S: Serializer>(
    collection: &CollectionVersion,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Input<'a> {
        definition: &'a CollectionVersion,
        root_id: u32,
    }
    Input {
        definition: collection,
        root_id: collection.root_id,
    }
    .serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<CollectionVersion, D::Error> {
    #[derive(Deserialize)]
    struct Input {
        definition: CollectionVersion,
        root_id: u32,
    }
    let mut input = Input::deserialize(deserializer)?;
    input.definition.root_id = input.root_id;
    Ok(input.definition)
}
