//! This library provides a key-value store that allows you to get, set and remove keys

#![deny(missing_docs)]

use std::collections::HashMap;

/// Main struct implementing key-value store functionality
pub struct KvStore {
    map: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    ///
    /// Used to create a new key-value store
    /// ```
    /// use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    ///
    /// Used to create a new key-value store
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    ///
    /// store.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
    /// ```
    pub fn get(&self, s: String) -> Option<String> {
        self.map.get(&s).cloned()
    }

    ///
    /// Used to create a new key-value store
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    ///
    /// store.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
    /// ```
    pub fn set(&mut self, k: String, v: String) {
        self.map.insert(k, v);
    }

    ///
    /// Used to create a new key-value store
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    ///
    /// store.set("key1".to_owned(), "value1".to_owned());
    /// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
    /// ```
    pub fn remove(&mut self, k: String) {
        self.map.remove(&k);
    }
}
