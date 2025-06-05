use either::Either;
use redb::{backends::InMemoryBackend, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow, collections::HashMap, convert::Infallible, fmt::Display, marker::PhantomData, ops::Deref, path::{Path, PathBuf}, sync::{Arc, Mutex, MutexGuard, RwLock}
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

    pub fn reader(&self) -> crate::Result<Transaction> {
        Transaction::reader(self.clone())
    }

    pub fn writer(&self) -> crate::Result<Transaction> {
        Transaction::writer(self.clone())
    }

    pub fn collection<T: Document>(&self, name: impl AsRef<str>) -> Collection<T> {
        Collection::<T>::new(self.clone(), name.as_ref().to_string())
    }
}

#[derive(Clone)]
pub enum Transaction {
    Read(Arc<RwLock<redb::ReadTransaction>>),
    Write(Arc<Mutex<redb::WriteTransaction>>)
}

impl Transaction {
    pub(crate) fn reader(db: Database) -> crate::Result<Self> {
        let txn = db.db().read()?.begin_read()?;
        Ok(Self::Read(Arc::new(RwLock::new(txn))))
    }

    pub(crate) fn writer(db: Database) -> crate::Result<Self> {
        let txn = db.db().read()?.begin_write()?;
        Ok(Self::Write(Arc::new(Mutex::new(txn))))
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

    fn index_table_names(&self) -> HashMap<String, String> {
        let mut results = HashMap::new();

        for key in T::index_keys() {
            results.insert(key.clone(), format!("collections/{}/index/{}", self.name(), key.clone()));
        }

        results
    }

    fn main_table_name(&self) -> String {
        format!("collections/{}", self.name())
    }

    pub(crate) fn database(&self) -> Database {
        self.database.clone()
    }
}

#[derive(Clone)]
pub(crate) struct CollectionOperation<T: Document> {
    operation: String,
    transaction: Transaction,
    database: Database,
    collection: Collection<T>
}

impl<T: Document> CollectionOperation<T> {
    pub fn new(operation: impl AsRef<str>, collection: &Collection<T>, transaction: &Transaction) -> Self {
        Self {
            operation: operation.as_ref().to_string(),
            transaction: transaction.clone(),
            database: collection.database(),
            collection: collection.clone()
        }
    }

    pub fn new_reader(operation: impl AsRef<str>, collection: &Collection<T>) -> crate::Result<Self> {
        Ok(Self::new(operation, collection, &Transaction::reader(collection.database())?))
    }

    pub fn new_writer(operation: impl AsRef<str>, collection: &Collection<T>) -> crate::Result<Self> {
        Ok(Self::new(operation, collection, &Transaction::writer(collection.database())?))
    }
}