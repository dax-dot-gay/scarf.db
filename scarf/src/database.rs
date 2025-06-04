use redb::{TableHandle, backends::InMemoryBackend};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    fmt::Display,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseLocation {
    InMemory,
    Filesystem(PathBuf),
}

impl DatabaseLocation {
    pub fn memory() -> Self {
        Self::InMemory
    }

    pub fn file(path: impl AsRef<Path>) -> Self {
        Self::Filesystem(path.as_ref().to_path_buf())
    }
}

#[derive(Clone)]
pub(crate) struct DatabaseInner {
    db: Arc<RwLock<redb::Database>>,
    location: DatabaseLocation,
}

impl DatabaseInner {
    pub fn new(location: DatabaseLocation) -> crate::Result<Self> {
        let created = match location.clone() {
            DatabaseLocation::InMemory => {
                redb::Database::builder().create_with_backend(InMemoryBackend::new())
            }
            DatabaseLocation::Filesystem(path) => redb::Database::create(path),
        }?;

        Ok(Self {
            db: Arc::new(RwLock::new(created)),
            location: location.clone(),
        })
    }

    pub fn location(&self) -> DatabaseLocation {
        self.location.clone()
    }

    pub fn create_reader(&self) -> crate::Result<redb::ReadTransaction> {
        let db = self.db.read()?;
        Ok(db.begin_read()?)
    }

    pub fn create_writer(&self) -> crate::Result<redb::WriteTransaction> {
        let db = self.db.read()?;
        Ok(db.begin_write()?)
    }

    pub fn list_tables(&self) -> crate::Result<Vec<String>> {
        let reader = self.create_reader()?;
        Ok(reader
            .list_tables()?
            .map(|t| t.name().to_string())
            .collect())
    }

    pub fn create_table<K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        name: impl AsRef<str>,
    ) -> crate::Result<bool> {
        let writer = self.create_writer()?;
        let existed = self.list_tables()?.contains(&name.as_ref().to_string());
        let _ = writer.open_table(redb::TableDefinition::<'_, K, V>::new(name.as_ref()))?;
        let _ = writer.commit()?;
        Ok(existed)
    }

    pub fn remove_table(&self, name: impl AsRef<str>) -> crate::Result<()> {
        let writer = self.create_writer()?;
        for table in writer.list_tables()? {
            if table.name() == name.as_ref() {
                writer.delete_table(table)?;
                return Ok(());
            }
        }

        Err(crate::Error::unknown_table(name.as_ref().to_string()))
    }

    pub fn insert<'k, 'v, K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        table: impl AsRef<str>,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> crate::Result<bool> {
        let writer = self.create_writer()?;
        let mut table =
            writer.open_table(redb::TableDefinition::<'_, K, V>::new(table.as_ref()))?;
        let result = table.insert(key, value)?;
        Ok(result.is_some())
    }
}
