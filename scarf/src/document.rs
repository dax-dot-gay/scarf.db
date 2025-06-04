use serde::{Serialize, de::DeserializeOwned};

pub trait Document: Serialize + DeserializeOwned {
    type PrimaryKey: redb::Key + Serialize + DeserializeOwned;

    fn id(&self) -> Self::PrimaryKey;
}
