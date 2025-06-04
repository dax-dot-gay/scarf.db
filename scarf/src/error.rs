use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unhandled redb error: {0:?}")]
    Redb(#[from] redb::Error),

    #[error("Filesystem/memory IO error: {0:?}")]
    Io(#[from] std::io::Error),

    #[error("Mutex poisoning error: {0}")]
    Poison(String),

    #[error("Unknown table name {0}")]
    UnknownTableName(String),

    #[error("More than one strong reference to this Arc exists: {0} strong, {1} weak.")]
    ArcReferences(usize, usize)
}

impl Error {
    pub fn unknown_table(name: impl AsRef<str>) -> Self {
        Self::UnknownTableName(name.as_ref().to_string())
    }

    pub fn arc_refs<T>(arc: Arc<T>) -> Self {
        Self::ArcReferences(Arc::strong_count(&arc), Arc::weak_count(&arc))
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Self::Poison(value.to_string())
    }
}

impl From<redb::CommitError> for Error {
    fn from(value: redb::CommitError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::CompactionError> for Error {
    fn from(value: redb::CompactionError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(value: redb::DatabaseError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::SavepointError> for Error {
    fn from(value: redb::SavepointError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::StorageError> for Error {
    fn from(value: redb::StorageError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::TableError> for Error {
    fn from(value: redb::TableError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(value: redb::TransactionError) -> Self {
        Self::Redb(value.into())
    }
}

impl From<redb::UpgradeError> for Error {
    fn from(value: redb::UpgradeError) -> Self {
        Self::Redb(value.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
