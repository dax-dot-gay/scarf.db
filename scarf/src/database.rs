use redb::{backends::InMemoryBackend, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow, collections::HashMap, fmt::Display, marker::PhantomData, ops::Deref, path::{Path, PathBuf}, sync::{Arc, Mutex, RwLock}
};

use crate::document::Document;

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

#[derive(Clone, Debug)]
pub struct Database {
    database: Arc<RwLock<redb::Database>>,
    location: DatabaseLocation
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let db = redb::Database::create(path.as_ref().to_path_buf())?;
        Ok(Self {
            database: Arc::new(RwLock::new(db)),
            location: DatabaseLocation::file(path)
        })
    }
    
    pub fn open_in_memory() -> crate::Result<Self> {
        let db = redb::Database::builder().create_with_backend(InMemoryBackend::new())?;
        Ok(Self {
            database: Arc::new(RwLock::new(db)),
            location: DatabaseLocation::memory()
        })
    }

    pub fn location(&self) -> DatabaseLocation {
        self.location.clone()
    }

    pub(crate) fn db(&self) -> Arc<RwLock<redb::Database>> {
        self.database.clone()
    }

    pub fn reader(&self) -> crate::Result<ReadTransaction> {
        ReadTransaction::new(self.db())
    }

    pub fn writer(&self) -> crate::Result<WriteTransaction> {
        WriteTransaction::new(self.db())
    }

    pub fn collection<T: Document>(&self, name: impl AsRef<str>) -> Collection<T> {
        Collection::<T>::new(self.clone(), name.as_ref().to_string())
    }
}

#[derive(Clone)]
pub struct WriteTransaction(Arc<Mutex<redb::WriteTransaction>>);

impl WriteTransaction {
    pub(crate) fn new(db: Arc<RwLock<redb::Database>>) -> crate::Result<Self> {
        let instance = db.read()?;
        Ok(Self(Arc::new(Mutex::new(instance.begin_write()?))))
    }

    pub fn txn(&self) -> Arc<Mutex<redb::WriteTransaction>> {
        self.0.clone()
    }

    pub fn commit(self) -> crate::Result<()> {
        let internal = Arc::try_unwrap(self.0).or_else(|e| Err(crate::Error::arc_refs(e)))?.into_inner()?;
        Ok(internal.commit()?)
    }

    pub fn abort(self) -> crate::Result<()> {
        let internal = Arc::try_unwrap(self.0).or_else(|e| Err(crate::Error::arc_refs(e)))?.into_inner()?;
        Ok(internal.abort()?)
    }
}

#[derive(Clone)]
pub struct ReadTransaction(Arc<Mutex<redb::ReadTransaction>>);

impl ReadTransaction {
    pub(crate) fn new(db: Arc<RwLock<redb::Database>>) -> crate::Result<Self> {
        let instance = db.read()?;
        Ok(Self(Arc::new(Mutex::new(instance.begin_read()?))))
    }

    pub fn txn(&self) -> Arc<Mutex<redb::ReadTransaction>> {
        self.0.clone()
    }

    pub fn close(self) -> crate::Result<()> {
        let internal = Arc::try_unwrap(self.0).or_else(|e| Err(crate::Error::arc_refs(e)))?.into_inner()?;
        Ok(internal.close()?)
    }
}

#[derive(Clone, Debug)]
pub struct Collection<T: Document> {
    database: Database,
    collection_name: String,
    doctype: PhantomData<T>,
    confirmed_existence: bool
}

impl<T: Document> Collection<T> {
    pub(crate) fn new(db: Database, name: String) -> Self {
        Self {
            database: db,
            collection_name: name,
            doctype: PhantomData,
            confirmed_existence: false
        }
    }

    pub fn name(&self) -> String {
        self.collection_name.clone()
    }

    fn index_tables(&self) -> HashMap<String, String> {
        let mut results = HashMap::new();

        for key in T::index_keys() {
            results.insert(key.clone(), format!("{}/index/{}", self.name(), key.clone()));
        }

        results
    }
}