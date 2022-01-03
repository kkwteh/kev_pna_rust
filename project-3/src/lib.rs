//! This library provides a key-value store that allows you to get, set and remove keys

#![deny(missing_docs)]

use byteorder::{BigEndian, ReadBytesExt};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use serde::{Deserialize, Serialize};
extern crate rmp_serde as rmps;

use rmps::Serializer;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};

use std::io::{self, prelude::*, SeekFrom};
use std::mem::size_of;

use sled::{self, Db};
use std::path::{Path, PathBuf};

const SIZE_OF_U64: u64 = size_of::<u64>() as u64;

/// enum representing a command
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum MPCommand {
    /// get command
    Get {
        /// key to get
        key: String,
    },
    /// set command
    Set {
        /// key to set
        key: String,
        /// value corresponding to key
        value: String,
    },
    /// rm command
    Rm {
        /// key to remove
        key: String,
    },
}

/// Main struct implementing key-value store functionality
pub struct KvStore {
    offset_map: HashMap<String, u64>,
    path: PathBuf,
    redundancies: u64,
}

/// Result type for KvStore
pub type Result<T> = std::result::Result<T, failure::Error>;

impl KvsEngine for KvStore {
    ///
    /// Gets a value from the key-value store
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    /// let mut store = KvStore::open(temp_dir.path()).unwrap();
    ///
    /// store.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    /// ```
    fn get(&mut self, k: String) -> Result<Option<String>> {
        {
            let opt_offset = self.offset_map.get(&k).cloned();
            match opt_offset {
                None => Ok(None),
                Some(offset) => {
                    let mut file = OpenOptions::new().read(true).open(self.path.as_path())?;
                    file.seek(SeekFrom::Start(offset as u64))?;
                    let mut buf: [u8; SIZE_OF_U64 as usize] = [0; SIZE_OF_U64 as usize];
                    file.read_exact(&mut buf)?;

                    let record_len = (&buf[0..SIZE_OF_U64 as usize]).read_u64::<BigEndian>()?;
                    let mut buf = vec![0u8; record_len.try_into()?];
                    file.read_exact(&mut buf)?;
                    let record: MPCommand = rmps::decode::from_read_ref(&buf)?;
                    match record {
                        MPCommand::Set { value, .. } => Ok(Some(value)),
                        _ => Err(failure::err_msg("Did not find set command where expected")),
                    }
                }
            }
        }
    }

    /// Used to set key in store
    fn set(&mut self, k: String, v: String) -> Result<()> {
        {
            let set_command = MPCommand::Set {
                key: k.clone(),
                value: v,
            };

            let mut buf = Vec::new();
            set_command.serialize(&mut Serializer::new(&mut buf))?;
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&self.path)?;

            let buf_len = (buf.len() as u64).to_be_bytes();

            let file_length = file.metadata()?.len();
            if self.offset_map.contains_key(&k) {
                self.redundancies += 1;
            }
            self.offset_map.insert(k, file_length);
            file.write_all(&buf_len).unwrap();
            file.write_all(&buf).unwrap();
        }
        // compact if redundancy level is high
        if self.redundancies as f64 > (self.offset_map.len() as f64 * 3.0) {
            self.compact()?;
        }

        Ok(())
    }

    /// Used to remove key from store
    fn remove(&mut self, k: String) -> Result<()> {
        {
            let opt_offset = self.offset_map.get(&k).cloned();
            match opt_offset {
                None => Err(failure::err_msg("Key not found")),
                Some(_offset) => {
                    let rm_command = MPCommand::Rm { key: k.clone() };

                    let mut buf = Vec::new();
                    rm_command.serialize(&mut Serializer::new(&mut buf))?;
                    let mut file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .append(true)
                        .open(&self.path)?;

                    self.offset_map.remove(&k);
                    self.redundancies += 2;

                    let buf_len = (buf.len() as u64).to_be_bytes();

                    file.write_all(&buf_len).unwrap();
                    file.write_all(&buf).unwrap();
                    Ok(())
                }
            }
        }
    }
}

impl KvStore {
    /// Opens the KvStore at the given location
    /// Used to create a new key-value store at the given path
    pub fn new() -> Result<Self> {
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        let path = Path::new(&rand_string);
        if !path.exists() {
            let _file = OpenOptions::new().create(true).write(true).open(&path)?;
        }

        let offset_map = HashMap::new();
        Ok(KvStore {
            path: path.to_owned(),
            offset_map,
            redundancies: 0,
        })
    }

    /// Opens the KvStore at the given location
    pub fn open(path: &Path) -> Result<Self> {
        let path = if path.is_dir() {
            path.join("my-file")
        } else {
            path.to_owned()
        };
        if !path.exists() {
            let _file = OpenOptions::new().create(true).write(true).open(&path)?;
        }

        let mut offset_map = HashMap::new();

        let mut offset: u64 = 0;
        let mut redundancies: u64 = 0;
        let mut file = OpenOptions::new().read(true).open(path.as_path())?;
        loop {
            let mut buf: [u8; SIZE_OF_U64 as usize] = [0; SIZE_OF_U64 as usize];
            let read_len_result = file.read_exact(&mut buf);

            match read_len_result {
                Ok(()) => {}
                Err(_err) => {
                    // TODO distinguish end of file
                    break;
                }
            }

            let record_len = (&buf[0..SIZE_OF_U64 as usize]).read_u64::<BigEndian>()?;
            let mut buf = vec![0u8; record_len.try_into()?];
            file.read_exact(&mut buf)?;
            let record: MPCommand = rmps::decode::from_read_ref(&buf)?;
            match record {
                MPCommand::Set { key, .. } => {
                    if offset_map.contains_key(&key) {
                        redundancies += 1;
                    }
                    offset_map.insert(key, offset);
                }
                MPCommand::Rm { key } => {
                    // one duplication for the redundant set, one for the unnecessary remove
                    redundancies += 2;
                    offset_map.remove(&key);
                }
                MPCommand::Get { key: _ } => {
                    return Err(failure::err_msg("found get command in file"));
                }
            }
            offset += record_len as u64 + SIZE_OF_U64;
        }

        Ok(KvStore {
            path,
            offset_map,
            redundancies,
        })
    }

    /// Compacts the KvStore file on disk
    pub fn compact(&mut self) -> Result<()> {
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        let mut new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&rand_string)?;
        let mut new_offset_map: HashMap<String, u64> = HashMap::new();
        let result: Result<()> = {
            let mut file = OpenOptions::new().read(true).open(self.path.as_path())?;
            let mut new_offset: u64 = 0;
            for (_k, offset) in self.offset_map.iter() {
                let mut buf_len: [u8; SIZE_OF_U64 as usize] = [0; SIZE_OF_U64 as usize];

                file.seek(SeekFrom::Start(*offset))?;
                file.read_exact(&mut buf_len)?;

                let record_len = (&buf_len[0..SIZE_OF_U64 as usize]).read_u64::<BigEndian>()?;
                let mut buf = vec![0u8; record_len.try_into()?];
                file.read_exact(&mut buf)?;
                let record: MPCommand = rmps::decode::from_read_ref(&buf)?;
                match record {
                    MPCommand::Set { key, .. } => {
                        new_file.write_all(&buf_len).unwrap();
                        new_file.write_all(&buf).unwrap();
                        new_offset_map.insert(key, new_offset);
                        new_offset += (buf_len.len() + buf.len()) as u64;
                    }
                    _ => return Err(failure::err_msg("Found invalid command at location")),
                }
            }
            self.offset_map = new_offset_map;
            fs::rename(&rand_string, &self.path)?;
            Ok(())
        };

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                fs::remove_file(&rand_string)?;
                Err(failure::err_msg(err.to_string()))
            }
        }
    }
}

/// client to send requests to KvsServer
pub struct KvsClient {}

/// serves responses to KvsClient
pub struct KvsServer {}

/// defines the storage interface called by KvsServer
pub trait KvsEngine {
    /// Set key-value pair in store
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Gets a value from store
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove key from store
    fn remove(&mut self, key: String) -> Result<()>;
}

/// KvsEngine implementation using sled crate
pub struct SledEngine {
    db: Db,
}

impl KvsEngine for SledEngine {
    /// Gets a value from the key-value store
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.db.get(&key)?;
        match value {
            None => Ok(None),
            Some(ivec) => {
                let value_string = std::str::from_utf8(&ivec)?.to_owned();
                Ok(Some(value_string))
            }
        }
    }

    /// Used to set key in store
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    /// Used to remove key from store
    fn remove(&mut self, key: String) -> Result<()> {
        let value = self.db.remove(&key)?;
        match value {
            None => {}
            Some(_ivec) => {}
        }
        Ok(())
    }
}

impl SledEngine {
    /// Opens the SledEngine at the given location
    pub fn open(path: &Path) -> Result<Self> {
        if !path.is_dir() {
            return Err(failure::err_msg("path is not directory"));
        };

        let db = sled::open(path)?;

        Ok(SledEngine { db })
    }
}

#[test]
fn test_sled() {
    use std::env::current_dir;
    let cwd = current_dir().unwrap();
    let mut sled = SledEngine::open(&cwd).unwrap();
    sled.set("foo".to_owned(), "bar".to_owned()).unwrap();
    let foo = sled.get("foo".to_owned()).unwrap();
    assert_eq!(foo, Some("bar".to_owned()));
}
