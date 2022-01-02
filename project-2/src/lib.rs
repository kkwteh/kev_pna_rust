//! This library provides a key-value store that allows you to get, set and remove keys

#![deny(missing_docs)]

use byteorder::{BigEndian, ReadBytesExt};
use failure;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use serde::{Deserialize, Serialize};
extern crate rmp_serde as rmps;
extern crate serde;

use rmps::Serializer;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};

use std::io::{prelude::*, SeekFrom};
use std::mem::size_of;

use std::path::{Path, PathBuf};

const SIZE_OF_U64: u64 = size_of::<u64>() as u64;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
enum MPCommand {
    Set { key: String, value: String },
    Rm { key: String },
}

/// Main struct implementing key-value store functionality
pub struct KvStore {
    offset_map: HashMap<String, u64>,
    path: PathBuf,
    redundancies: u64,
}

/// Result type for KvStore
pub type Result<T> = std::result::Result<T, failure::Error>;

impl KvStore {
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
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
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
                        MPCommand::Rm { .. } => Err(failure::err_msg("Found rm instead of set")),
                    }
                }
            }
        }
    }

    /// Used to set key in store
    pub fn set(&mut self, k: String, v: String) -> Result<()> {
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
    pub fn remove(&mut self, k: String) -> Result<()> {
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

    ///
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
            }
            offset += record_len as u64 + SIZE_OF_U64;
        }

        Ok(KvStore {
            path: path.to_owned(),
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
                    MPCommand::Rm { .. } => {
                        return Err(failure::err_msg("Found rm command at set location"))
                    }
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
