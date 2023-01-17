//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::stream::StreamExt;
use futures_util::future;
use futures_util::TryStreamExt;
use opendal::ObjectMode;
use opendal::Operator;
use storages_common_table_meta::caches::LoadParams;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotLite;
use tracing::info;
use tracing::warn;
use tracing::Instrument;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;

// Read snapshot related operations.
pub struct SnapshotsIO {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    format_version: u64,
}

pub struct SnapshotLiteListExtended {
    pub chained_snapshot_lites: Vec<TableSnapshotLite>,
    pub segment_locations: HashMap<Location, HashSet<SnapshotId>>,
    pub orphan_snapshot_lites: Vec<TableSnapshotLite>,
}

pub enum ListSnapshotLiteOption<'a> {
    // do not case about the segments
    NeedNotSegments,
    // need segment, and exclude the locations if Some(Hashset<Location>) is provided
    NeedSegmentsWithExclusion(Option<&'a HashSet<Location>>),
}

impl SnapshotsIO {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator, format_version: u64) -> Self {
        Self {
            ctx,
            operator,
            format_version,
        }
    }

    async fn read_snapshot(
        snapshot_location: String,
        format_version: u64,
        data_accessor: Operator,
    ) -> Result<Arc<TableSnapshot>> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver: format_version,
            schema: None,
        };
        reader.read(&load_params).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_snapshots(
        &self,
        snapshot_files: &[String],
    ) -> Result<Vec<Result<Arc<TableSnapshot>>>> {
        let ctx = self.ctx.clone();
        let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        // 1.1 combine all the tasks.
        let mut iter = snapshot_files.iter();
        let tasks = std::iter::from_fn(move || {
            if let Some(location) = iter.next() {
                let location = location.clone();
                Some(
                    Self::read_snapshot(location, self.format_version, self.operator.clone())
                        .instrument(tracing::debug_span!("read_snapshot")),
                )
            } else {
                None
            }
        });

        // 1.2 build the runtime.
        let semaphore = Semaphore::new(max_io_requests);
        let snapshot_runtime = Arc::new(Runtime::with_worker_threads(
            max_runtime_threads,
            Some("fuse-req-snapshots-worker".to_owned()),
        )?);

        // 1.3 spawn all the tasks to the runtime.
        let join_handlers = snapshot_runtime.try_spawn_batch(semaphore, tasks).await?;

        // 1.4 get all the result.
        future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("read snapshots failure, {}", e)))
    }

    // Read all the table statistic files by the root file(exclude the root file).
    // limit: read how many table statistic files
    pub async fn read_table_statistic_files(
        &self,
        root_ts_file: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>> {
        // Get all file list.
        if let Some(prefix) = Self::get_s3_prefix_from_file(root_ts_file) {
            return self.list_files(&prefix, limit, Some(root_ts_file)).await;
        }
        Ok(vec![])
    }

    // read all the precedent snapshots of given `root_snapshot`
    pub async fn read_chained_snapshot_lites(
        &self,
        location_generator: TableMetaLocationGenerator,
        root_snapshot: String,
        limit: Option<usize>,
    ) -> Result<Vec<TableSnapshotLite>> {
        let table_snapshot_reader =
            MetaReaders::table_snapshot_reader(self.ctx.get_data_operator()?.operator());
        let lite_snapshot_stream = table_snapshot_reader
            .snapshot_history(root_snapshot, self.format_version, location_generator)
            .map_ok(|snapshot| TableSnapshotLite::from(snapshot.as_ref()));
        if let Some(l) = limit {
            lite_snapshot_stream.take(l).try_collect::<Vec<_>>().await
        } else {
            lite_snapshot_stream.try_collect::<Vec<_>>().await
        }
    }

    // Read all the snapshots by the root file.
    // limit: limits the number of snapshot files listed
    // with_segment_locations: if true will get the segments of the snapshot
    pub async fn read_snapshot_lites_ext<'a, T>(
        &self,
        root_snapshot_file: String,
        limit: Option<usize>,
        list_options: ListSnapshotLiteOption<'a>,
        min_snapshot_timestamp: Option<DateTime<Utc>>,
        status_callback: T,
    ) -> Result<SnapshotLiteListExtended>
    where
        T: Fn(String),
    {
        let ctx = self.ctx.clone();
        let data_accessor = self.operator.clone();

        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let mut snapshot_files = vec![];
        let mut segment_location_with_index: HashMap<Location, HashSet<SnapshotId>> =
            HashMap::new();
        if let Some(prefix) = Self::get_s3_prefix_from_file(&root_snapshot_file) {
            snapshot_files = self.list_files(&prefix, limit, None).await?;
        }

        // 1. Get all the snapshot by chunks.
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
        let mut snapshot_lites = Vec::with_capacity(snapshot_files.len());

        let start = Instant::now();
        let mut count = 0;
        for chunk in snapshot_files.chunks(max_io_requests) {
            let results = self.read_snapshots(chunk).await?;

            for snapshot in results.into_iter().flatten() {
                if snapshot.timestamp > min_snapshot_timestamp {
                    // filter out snapshots which have larger (artificial)timestamp , they are
                    // not members of precedents of the current snapshot, whose timestamp is
                    // min_snapshot_timestamp.
                    //
                    // NOTE: it is NOT the case that all those have lesser timestamp, are
                    // members of precedents of the current snapshot, though.
                    continue;
                }
                let snapshot_lite = TableSnapshotLite::from(snapshot.as_ref());
                let snapshot_id = snapshot_lite.snapshot_id;
                snapshot_lites.push(snapshot_lite);

                if let ListSnapshotLiteOption::NeedSegmentsWithExclusion(filter) = list_options {
                    // collects segments, and the snapshots that reference them.
                    for segment_location in &snapshot.segments {
                        if let Some(excludes) = filter {
                            if excludes.contains(segment_location) {
                                continue;
                            }
                        }
                        segment_location_with_index
                            .entry(segment_location.clone())
                            .and_modify(|v| {
                                v.insert(snapshot_id);
                            })
                            .or_insert_with(|| HashSet::from_iter(vec![snapshot_id]));
                    }
                }
            }

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "gc: read snapshot files:{}/{}, cost:{} sec",
                    count,
                    snapshot_files.len(),
                    start.elapsed().as_secs()
                );
                info!(status);
                (status_callback)(status);
            }
        }

        let root_snapshot = Self::read_snapshot(
            root_snapshot_file.clone(),
            self.format_version,
            data_accessor.clone(),
        )
        .await?;

        let (chained_snapshot_lites, orphan_snapshot_lites) =
            Self::chain_snapshots(snapshot_lites, &root_snapshot);

        Ok(SnapshotLiteListExtended {
            chained_snapshot_lites,
            segment_locations: segment_location_with_index,
            orphan_snapshot_lites,
        })
    }

    fn chain_snapshots(
        snapshot_lites: Vec<TableSnapshotLite>,
        root_snapshot: &TableSnapshot,
    ) -> (Vec<TableSnapshotLite>, Vec<TableSnapshotLite>) {
        let mut snapshot_map = HashMap::new();
        let mut chained_snapshot_lites = vec![];
        for snapshot_lite in snapshot_lites.into_iter() {
            snapshot_map.insert(snapshot_lite.snapshot_id, snapshot_lite);
        }
        let root_snapshot_lite = TableSnapshotLite::from(root_snapshot);
        let mut prev_snapshot_id_tuple = root_snapshot_lite.prev_snapshot_id;
        chained_snapshot_lites.push(root_snapshot_lite);
        while let Some((prev_snapshot_id, _)) = prev_snapshot_id_tuple {
            let prev_snapshot_lite = snapshot_map.remove(&prev_snapshot_id);
            match prev_snapshot_lite {
                None => {
                    break;
                }
                Some(prev_snapshot) => {
                    prev_snapshot_id_tuple = prev_snapshot.prev_snapshot_id;
                    chained_snapshot_lites.push(prev_snapshot);
                }
            }
        }
        // remove root from orphan list
        snapshot_map.remove(&root_snapshot.snapshot_id);
        (chained_snapshot_lites, snapshot_map.into_values().collect())
    }

    async fn list_files(
        &self,
        prefix: &str,
        limit: Option<usize>,
        exclude_file: Option<&str>,
    ) -> Result<Vec<String>> {
        let data_accessor = self.operator.clone();

        let mut file_list = vec![];
        let mut ds = data_accessor.object(prefix).list().await?;
        while let Some(de) = ds.try_next().await? {
            match de.mode().await? {
                ObjectMode::FILE => match exclude_file {
                    Some(path) if de.path() == path => continue,
                    _ => {
                        let location = de.path().to_string();
                        let modified = de.last_modified().await?;
                        file_list.push((location, modified));
                    }
                },
                _ => {
                    warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                    continue;
                }
            }
        }

        // let mut vector: Vec<(i32, Option<i32>)> = vec![(1, Some(1)), (2, None), (3, Some(3)), (4, None)];
        // vector.sort_by(|(_, k1), (_, k2)| k2.cmp(k1));
        // Result:
        // [(3, Some(3)), (1, Some(1)), (2, None),(4, None)]
        file_list.sort_by(|(_, m1), (_, m2)| m2.cmp(m1));

        Ok(match limit {
            None => file_list.into_iter().map(|v| v.0).collect::<Vec<String>>(),
            Some(v) => file_list
                .into_iter()
                .take(v)
                .map(|v| v.0)
                .collect::<Vec<String>>(),
        })
    }

    // _ss/xx/yy.json -> _ss/xx/
    fn get_s3_prefix_from_file(file_path: &str) -> Option<String> {
        if let Some(path) = Path::new(&file_path).parent() {
            let mut prefix = path.to_str().unwrap_or("").to_string();

            if !prefix.contains('/') {
                return None;
            }

            // Append '/' to the end if need.
            if !prefix.ends_with('/') {
                prefix += "/";
            }
            return Some(prefix);
        }
        None
    }
}
