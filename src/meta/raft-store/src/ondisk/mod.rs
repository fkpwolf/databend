// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This mod defines on-disk data version, the storage of the data version, and provide upgrade functions.

mod data_version;
mod header;
pub(crate) mod version_info;

use std::collections::BTreeSet;
use std::fmt;

use common_meta_sled_store::sled;
use common_meta_sled_store::SledTree;
use common_meta_stoerr::MetaStorageError;
pub use data_version::DataVersion;
pub use header::Header;
use tracing::info;

use crate::config::RaftConfig;
use crate::key_spaces::DataHeader;
use crate::key_spaces::RaftStoreEntry;
use crate::key_spaces::RaftStoreEntryCompat;
use crate::log::TREE_RAFT_LOG;
use crate::state::TREE_RAFT_STATE;

/// The sled tree name to store the data versions.
pub const TREE_HEADER: &str = "header";

/// The working data version the program runs on
pub static DATA_VERSION: DataVersion = DataVersion::V001;

/// On disk data descriptor.
///
/// It should be loaded before accessing other data on disk.
/// And if the data is upgrading, it should be upgraded before accessing other data on disk.
/// If the on disk data is an old version, it should be upgraded to the current version.
#[derive(Debug, Clone)]
pub struct OnDisk {
    pub header: Header,

    #[allow(dead_code)]
    db: sled::Db,

    config: RaftConfig,

    log_stderr: bool,
}

impl fmt::Display for OnDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "header: {:?}, data-dir: {}",
            self.header, self.config.raft_dir
        )
    }
}

impl OnDisk {
    const KEY_HEADER: &'static str = "header";

    /// Initialize data version for local store, returns the loaded version.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn open(db: &sled::Db, config: &RaftConfig) -> Result<OnDisk, MetaStorageError> {
        info!(?config, "open and initialize data-version");

        let tree_name = config.tree_name(TREE_HEADER);
        let tree = SledTree::open(db, &tree_name, config.is_sync())?;
        let ks = tree.key_space::<DataHeader>();

        let header = ks.get(&Self::KEY_HEADER.to_string())?;
        info!("Loaded header: {:?}", header);

        if let Some(v) = header {
            return Ok(OnDisk::new(v, db, config));
        }

        // Without header, by default it is V0.

        let header = Header {
            version: DataVersion::V0,
            upgrading: None,
        };
        ks.insert(&Self::KEY_HEADER.to_string(), &header).await?;

        Ok(OnDisk::new(header, db, config))
    }

    fn new(header: Header, db: &sled::Db, config: &RaftConfig) -> Self {
        let min_compatible = DATA_VERSION.min_compatible_data_version();

        if header.version < min_compatible {
            let max_compatible = header.version.max_compatible_working_version();
            let version_info = min_compatible.version_info();

            eprintln!("Working data version is: {}", DATA_VERSION);
            eprintln!("On-disk data version is too old: {}", header.version);
            eprintln!("The latest compatible version is {}", max_compatible);
            eprintln!(
                "Download the latest compatible version: {}",
                version_info.download_url()
            );

            panic!(
                "On-disk data version {} is too old, the latest compatible version is {}.",
                header.version, max_compatible
            );
        }

        Self {
            header,
            db: db.clone(),
            config: config.clone(),
            log_stderr: false,
        }
    }

    /// Enable or disable logging crucial steps to stderr, when upgrading.
    pub fn log_stderr(&mut self, log_stderr: bool) {
        self.log_stderr = log_stderr;
    }

    /// Upgrade the on-disk data to latest version `DATA_VERSION`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upgrade(&mut self) -> Result<(), MetaStorageError> {
        if let Some(u) = self.header.upgrading {
            self.progress(format_args!("Found unfinished upgrading: {:?}", u));

            match u {
                DataVersion::V0 => {
                    unreachable!("Upgrading to V0 is not supported");
                }
                DataVersion::V001 => {
                    self.progress(format_args!(
                        "Upgrading to V001 does not need to cleanup. Data are upgraded in place"
                    ));
                }
            }

            self.header.upgrading = None;
            self.write_header(&self.header).await?;
            self.progress(format_args!("Cleared upgrading flag"));
        }

        while self.header.version != DATA_VERSION {
            match self.header.version {
                DataVersion::V0 => {
                    self.upgrade_v0_to_v001().await?;
                }
                DataVersion::V001 => {
                    unreachable!("{} is the latest version", self.header.version)
                }
            }
        }

        Ok(())
    }

    /// Upgrade the on-disk data form [`DataVersion::V0`] to [`DataVersion::V001`].
    ///
    /// `V0` data is openraft-v7 and v8 compatible.
    /// `V001` data is only openraft-v8 compatible.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn upgrade_v0_to_v001(&mut self) -> Result<(), MetaStorageError> {
        assert_eq!(DataVersion::V0, self.header.version);

        let next = self.header.version.next().unwrap();

        self.progress(format_args!("Upgrade on-disk data"));
        self.progress(format_args!("    From: {:?}", self.header.version));
        self.progress(format_args!("    To:   {:?}", next));

        // 1. Set upgrading flag indicating the upgrading is in progress.
        //    When it crashes before upgrading finishes, it can redo the upgrading.
        self.begin_upgrading().await?;

        // 2. Upgrade data

        let mut tree_names = self.tree_names().await?;
        if Some(TREE_HEADER) == tree_names.first().map(|x| x.as_str()) {
            tree_names.remove(0);
        }

        let mut cnt = 0;
        for tree_name in tree_names {
            let tree = self.db.open_tree(tree_name)?;

            for ivec_pair_res in tree.iter() {
                let kv_entry = {
                    let (k_ivec, v_ivec) = ivec_pair_res?;
                    // `deserialize` is able to load both openraft-v07 and openraft-v08 data.
                    // And then serialize it to openraft-v08 data.
                    RaftStoreEntryCompat::deserialize(&k_ivec, &v_ivec)?
                };

                tracing::debug!(
                    kv_entry = debug(&kv_entry),
                    "upgrade kv from {:?}",
                    self.header.version
                );

                let (k, v) = RaftStoreEntry::serialize(&kv_entry)?;
                tree.insert(k, v)?;
                cnt += 1;
            }

            tree.flush()?;
        }

        // 3. Finish upgrading: clear upgrading flag

        self.progress(format_args!("Upgraded {} records", cnt));

        self.finish_upgrading().await?;

        Ok(())
    }

    async fn tree_names(&self) -> Result<Vec<String>, MetaStorageError> {
        let mut present_tree_names = {
            let mut tree_names = BTreeSet::new();
            for n in self.db.tree_names() {
                let name = String::from_utf8(n.to_vec())?;
                tree_names.insert(name);
            }
            tree_names
        };

        // Export in header, raft_state, log and other order.
        let mut tree_names = vec![];

        for name in [TREE_HEADER, TREE_RAFT_STATE, TREE_RAFT_LOG] {
            if present_tree_names.remove(name) {
                tree_names.push(name.to_string());
            } else {
                self.progress(format_args!("tree {} not found", name));
            }
        }
        tree_names.extend(present_tree_names.into_iter().collect::<Vec<_>>());

        Ok(tree_names)
    }

    /// Set upgrading flag indicating the upgrading is in progress.
    ///
    /// When it crashes before upgrading finishes, it can redo the upgrading.
    async fn begin_upgrading(&mut self) -> Result<(), MetaStorageError> {
        assert!(self.header.upgrading.is_none(), "can not upgrade twice");

        self.header.upgrading = self.header.version.next();
        self.progress(format_args!("Begin upgrading: {}", self.header));

        self.write_header(&self.header).await?;
        Ok(())
    }

    /// Reset upgrading flag indicating the upgrading is finished, and set header.version to next version.
    async fn finish_upgrading(&mut self) -> Result<(), MetaStorageError> {
        self.header.version = self.header.upgrading.unwrap();
        self.header.upgrading = None;
        self.progress(format_args!("Finished upgrading: {}", self.header));

        self.write_header(&self.header).await?;
        Ok(())
    }

    async fn write_header(&self, header: &Header) -> Result<(), MetaStorageError> {
        let tree = self.header_tree()?;
        let ks = tree.key_space::<DataHeader>();

        ks.insert(&Self::KEY_HEADER.to_string(), header).await?;

        self.progress(format_args!("Write header: {}", header));
        Ok(())
    }

    #[allow(dead_code)]
    fn read_header(&self) -> Result<Option<Header>, MetaStorageError> {
        let tree = self.header_tree()?;
        let ks = tree.key_space::<DataHeader>();

        let header = ks.get(&Self::KEY_HEADER.to_string())?;
        Ok(header)
    }

    fn header_tree(&self) -> Result<SledTree, MetaStorageError> {
        let tree_name = self.config.tree_name(TREE_HEADER);
        SledTree::open(&self.db, tree_name, self.config.is_sync())
    }

    fn progress(&self, s: impl fmt::Display) {
        if self.log_stderr {
            eprintln!("{}", s);
        }

        info!("{}", s);
    }
}
