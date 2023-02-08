// Copyright 2021 Datafuse Labs.
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

use std::fmt::Debug;
use std::io::Cursor;
use std::io::ErrorKind;
use std::ops::RangeBounds;

use anyerror::AnyError;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::sync::RwLockWriteGuard;
use common_exception::WithContext;
use common_meta_raft_store::applied_state::AppliedState;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::SerializableSnapshot;
use common_meta_raft_store::state_machine::Snapshot;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::storage::LogState;
use common_meta_sled_store::openraft::EffectiveMembership;
use common_meta_sled_store::openraft::ErrorSubject;
use common_meta_sled_store::openraft::ErrorVerb;
use common_meta_sled_store::openraft::Membership;
use common_meta_sled_store::openraft::StateMachineChanges;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::Endpoint;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaStartupError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use openraft::async_trait::async_trait;
use openraft::raft::Entry;
use openraft::storage::HardState;
use openraft::LogId;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::export::vec_kv_to_json;
use crate::metrics::raft_metrics;
use crate::metrics::server_metrics;
use crate::store::ToStorageError;
use crate::Opened;

/// An storage implementing the `async_raft::RaftStorage` trait.
///
/// It is the stateful part in a raft impl.
/// This store is backed by a sled db, contents are stored in 3 trees:
///   state:
///       id
///       hard_state
///   log
///   state_machine
pub struct RaftStoreBare {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from fs) or created.
    is_opened: bool,

    /// The sled db for log and raft_state.
    /// state machine is stored in another sled db since it contains user data and needs to be export/import as a whole.
    /// This db is also used to generate a locally unique id.
    /// Currently the id is used to create a unique snapshot id.
    pub(crate) db: sled::Db,

    // Raft state includes:
    // id: NodeId,
    //     current_term,
    //     voted_for
    pub raft_state: RaftState,

    pub log: RaftLog,

    /// The Raft state machine.
    ///
    /// sled db has its own concurrency control, e.g., batch or transaction.
    /// But we still need a lock, when installing a snapshot, which is done by replacing the state machine:
    ///
    /// - Acquire a read lock to WRITE or READ. Transactional RW relies on sled concurrency control.
    /// - Acquire a write lock before installing a snapshot, to prevent any write to the db.
    pub state_machine: RwLock<StateMachine>,

    /// The current snapshot.
    pub current_snapshot: RwLock<Option<Snapshot>>,
}

impl AsRef<RaftStoreBare> for RaftStoreBare {
    fn as_ref(&self) -> &RaftStoreBare {
        self
    }
}

impl Opened for RaftStoreBare {
    /// If the instance is opened(true) from an existent state(e.g. load from fs) or created(false).
    fn is_opened(&self) -> bool {
        self.is_opened
    }
}

impl RaftStoreBare {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[tracing::instrument(level = "debug", skip_all, fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<RaftStoreBare, MetaStartupError> {
        info!("open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        info!("RaftLog opened");

        let (sm_id, prev_sm_id) = raft_state.read_state_machine_id()?;

        // There is a garbage state machine need to be cleaned.
        if sm_id != prev_sm_id {
            StateMachine::clean(config, prev_sm_id)?;
            raft_state.write_state_machine_id(&(sm_id, sm_id)).await?;
        }

        let sm = RwLock::new(StateMachine::open(config, sm_id).await?);
        let current_snapshot = RwLock::new(None);

        Ok(Self {
            id: raft_state.id,
            config: config.clone(),
            is_opened: is_open,
            db,
            raft_state,
            log,
            state_machine: sm,
            current_snapshot,
        })
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, StateMachine> {
        self.state_machine.write().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id))]
    async fn do_build_snapshot(
        &self,
    ) -> Result<
        openraft::storage::Snapshot<
            <RaftStoreBare as RaftStorage<LogEntry, AppliedState>>::SnapshotData,
        >,
        StorageError,
    > {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        // 1. Take a serialized snapshot

        let (snap, last_applied_log, snapshot_id) = match self
            .state_machine
            .write()
            .await
            .build_snapshot()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("build_snapshot", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let data = serde_json::to_vec(&snap)
            .map_err(MetaStorageError::from)
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;

        let snapshot_size = data.len();

        let snap_meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id: snapshot_id.to_string(),
        };

        let snapshot = Snapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };

        // Update the snapshot first.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        info!(snapshot_size = snapshot_size, "log compaction complete");

        Ok(openraft::storage::Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[tracing::instrument(level = "debug", skip(self, data))]
    pub async fn install_snapshot(&self, data: &[u8]) -> Result<(), MetaStorageError> {
        let mut sm = self.state_machine.write().await;

        let (sm_id, prev_sm_id) = self.raft_state.read_state_machine_id()?;
        if sm_id != prev_sm_id {
            return Err(MetaStorageError::SnapshotError(AnyError::error(format!(
                "another snapshot install is not finished yet: {} {}",
                sm_id, prev_sm_id
            ))));
        }

        let new_sm_id = sm_id + 1;

        info!("snapshot data len: {}", data.len());

        let snap: SerializableSnapshot = serde_json::from_slice(data)?;

        // If not finished, clean up the new tree.
        self.raft_state
            .write_state_machine_id(&(sm_id, new_sm_id))
            .await?;

        let new_sm = StateMachine::open(&self.config, new_sm_id).await?;
        info!(
            "insert all key-value into new state machine, n={}",
            snap.kvs.len()
        );

        let tree = &new_sm.sm_tree.tree;
        let nkvs = snap.kvs.len();
        for x in snap.kvs.into_iter() {
            let k = &x[0];
            let v = &x[1];
            tree.insert(k, v.clone()).context(|| "insert snapshot")?;
        }

        info!(
            "installed state machine from snapshot, no_kvs: {} last_applied: {:?}",
            nkvs,
            new_sm.get_last_applied()?,
        );

        tree.flush_async().await.context(|| "flush snapshot")?;

        info!("flushed tree, no_kvs: {}", nkvs);

        // Start to use the new tree, the old can be cleaned.
        self.raft_state
            .write_state_machine_id(&(new_sm_id, sm_id))
            .await?;

        info!(
            "installed state machine from snapshot, last_applied: {:?}",
            new_sm.get_last_applied()?,
        );

        StateMachine::clean(&self.config, sm_id)?;

        self.raft_state
            .write_state_machine_id(&(new_sm_id, new_sm_id))
            .await?;

        // TODO(xp): use checksum to check consistency?

        *sm = new_sm;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn export(&self) -> Result<Vec<String>, std::io::Error> {
        let mut res = vec![];

        let state_kvs = self
            .raft_state
            .inner
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        for kv in state_kvs.iter() {
            let line = vec_kv_to_json(&self.raft_state.inner.name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        let log_kvs = self
            .log
            .inner
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
        for kv in log_kvs.iter() {
            let line = vec_kv_to_json(&self.log.inner.name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        let name = self.state_machine.write().await.sm_tree.name.clone();
        let sm_kvs = self
            .state_machine
            .write()
            .await
            .sm_tree
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        for kv in sm_kvs.iter() {
            let line = vec_kv_to_json(&name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        Ok(res)
    }
}

#[async_trait]
impl RaftStorage<LogEntry, AppliedState> for RaftStoreBare {
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "debug", skip(self, hs), fields(id=self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<(), StorageError> {
        info!("save_hard_state: {:?}", hs);

        match self
            .raft_state
            .write_hard_state(hs)
            .await
            .map_to_sto_err(ErrorSubject::HardState, ErrorVerb::Write)
        {
            Err(err) => {
                return {
                    raft_metrics::storage::incr_raft_storage_fail("save_hard_state", true);
                    Err(err)
                };
            }
            Ok(_) => return Ok(()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RB,
    ) -> Result<Vec<Entry<LogEntry>>, StorageError> {
        debug!("try_get_log_entries: range: {:?}", range);

        match self
            .log
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Ok(entries) => return Ok(entries),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("try_get_log_entries", false);
                Err(err)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn delete_conflict_logs_since(&self, log_id: LogId) -> Result<(), StorageError> {
        info!("delete_conflict_logs_since: {}", log_id);

        match self
            .log
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("delete_conflict_logs_since", true);
                Err(err)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn purge_logs_upto(&self, log_id: LogId) -> Result<(), StorageError> {
        info!("purge_logs_upto: {}", log_id);

        if let Err(err) = self
            .log
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        };
        if let Err(err) = self
            .log
            .range_remove(..=log_id.index)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
    async fn append_to_log(&self, entries: &[&Entry<LogEntry>]) -> Result<(), StorageError> {
        for ent in entries {
            info!("append_to_log: {}", ent.log_id);
        }

        let entries = entries.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
        match self
            .log
            .append(&entries)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("append_to_log", true);
                Err(err)
            }
            Ok(_) => return Ok(()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<LogEntry>],
    ) -> Result<Vec<AppliedState>, StorageError> {
        for ent in entries {
            info!("apply_to_state_machine: {}", ent.log_id);
        }

        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;
        for entry in entries {
            let r = match sm
                .apply(entry)
                .await
                .map_to_sto_err(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)
            {
                Err(err) => {
                    raft_metrics::storage::incr_raft_storage_fail("apply_to_state_machine", true);
                    return Err(err);
                }
                Ok(r) => r,
            };
            res.push(r);
        }
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn build_snapshot(
        &self,
    ) -> Result<openraft::storage::Snapshot<Self::SnapshotData>, StorageError> {
        self.do_build_snapshot().await
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn begin_receiving_snapshot(&self) -> Result<Box<Self::SnapshotData>, StorageError> {
        server_metrics::incr_applying_snapshot(1);
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "debug", skip(self, snapshot), fields(id=self.id))]
    async fn install_snapshot(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges, StorageError> {
        // TODO(xp): disallow installing a snapshot with smaller last_applied.

        info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
        server_metrics::incr_applying_snapshot(-1);

        let new_snapshot = Snapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        info!("snapshot meta: {:?}", meta);

        // Replace state machine with the new one
        let res = self.install_snapshot(&new_snapshot.data).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                raft_metrics::storage::incr_raft_storage_fail("install_snapshot", true);
                error!("error: {:?} when install_snapshot", e);
            }
        };

        // Update current snapshot.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<openraft::storage::Snapshot<Self::SnapshotData>>, StorageError> {
        info!("get snapshot start");
        let snap = match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(openraft::storage::Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        };

        info!("get snapshot complete");

        snap
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_hard_state(&self) -> Result<Option<HardState>, StorageError> {
        match self
            .raft_state
            .read_hard_state()
            .map_to_sto_err(ErrorSubject::HardState, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("read_hard_state", false);
                return Err(err);
            }
            Ok(hard_state) => return Ok(hard_state),
        }
    }

    async fn get_log_state(&self) -> Result<LogState, StorageError> {
        let last_purged_log_id = match self
            .log
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last = match self
            .log
            .logs()
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x.1.log_id),
        };

        debug!(
            "get_log_state: ({:?},{:?}]",
            last_purged_log_id, last_log_id
        );

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn last_applied_state(
        &self,
    ) -> Result<(Option<LogId>, Option<EffectiveMembership>), StorageError> {
        let sm = self.state_machine.read().await;
        let last_applied = match sm
            .get_last_applied()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("last_applied_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };
        let last_membership = match sm
            .get_membership()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("last_applied_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        debug!(
            "last_applied_state: applied: {:?}, membership: {:?}",
            last_applied, last_membership
        );

        Ok((last_applied, last_membership))
    }
}

impl RaftStoreBare {
    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, MetaError> {
        let sm = self.state_machine.read().await;

        let n = sm.get_node(node_id)?;
        Ok(n)
    }

    /// Return a list of nodes for which the `predicate` returns true.
    pub async fn get_nodes(
        &self,
        predicate: impl Fn(&Membership, &NodeId) -> bool,
    ) -> Result<Vec<Node>, MetaStorageError> {
        let sm = self.state_machine.read().await;
        let ms = sm.get_membership()?;

        let membership = match ms {
            Some(membership) => membership.membership,
            None => return Ok(vec![]),
        };

        let nodes = sm.nodes().range(..)?;
        let mut ns = vec![];

        for x in nodes {
            let item = x?;
            if predicate(&membership, &item.key()?) {
                ns.push(item.value()?);
            }
        }

        Ok(ns)
    }

    pub async fn get_node_endpoint(&self, node_id: &NodeId) -> Result<Endpoint, MetaError> {
        let endpoint = self
            .get_node(node_id)
            .await?
            .map(|n| n.endpoint)
            .ok_or_else(|| MetaNetworkError::GetNodeAddrError(format!("node id: {}", node_id)))?;

        Ok(endpoint)
    }
}
