pub mod compaction;
mod map;
mod sstable;

pub use self::map::LsmMap;

use self::sstable::{SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use bincode;
use std::error;
use std::fmt;
use std::io;
use std::result;

/// Convenience `Error` enum for `lsm_tree`.
#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    SerdeError(bincode::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::SerdeError(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::IOError(ref error) => error.description(),
            Error::SerdeError(ref error) => error.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            Error::IOError(ref error) => error.cause(),
            Error::SerdeError(ref error) => error.cause(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IOError(ref error) => write!(f, "{}", error),
            Error::SerdeError(ref error) => write!(f, "{}", error),
        }
    }
}

/// Convenience `Result` type for `lsm_tree`.
pub type Result<T> = result::Result<T, Error>;
