use std::{collections::HashMap, fmt::Debug};

use redb::TypeName;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use base64::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(uuid::Uuid);

impl redb::Value for Id {
    type SelfType<'a> = Id;
    type AsBytes<'a> = [u8; 16];

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
        where
            Self: 'b {
        u128::as_bytes(&value.0.as_u128())
    }

    fn fixed_width() -> Option<usize> {
        Some(16)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a {
        Self(uuid::Uuid::from_u128(u128::from_bytes(data)))
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("scarf::Id")
    }
}

impl redb::Key for Id {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        u128::compare(data1, data2)
    }
}

pub trait Document: Serialize + DeserializeOwned + Clone + Debug {
    type PrimaryKey: redb::Key + Serialize + DeserializeOwned;

    fn id(&self) -> Self::PrimaryKey;
    fn id_field() -> String;
    fn index_keys() -> Vec<String>;
    fn index_vals(&self) -> HashMap<String, rmpv::Value>;

    fn serialized_indices(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();

        for (key, val) in self.index_vals() {
            let mut writer = Vec::<u8>::new();
            rmpv::encode::write_value(&mut writer, &val).unwrap();
            result.insert(key, BASE64_URL_SAFE_NO_PAD.encode(writer.as_slice()));
        }

        result
    }
}
