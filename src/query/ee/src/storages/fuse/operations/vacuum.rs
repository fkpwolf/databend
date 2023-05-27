// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::SnapshotLiteExtended;
use common_storages_fuse::io::SnapshotsIO;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::FuseTable;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::CompactSegmentInfo;
use tracing::info;

use crate::storages::fuse::get_snapshot_referenced_segments;

#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotReferencedFiles {
    pub segments: HashSet<String>,
    pub blocks: HashSet<String>,
    pub blocks_index: HashSet<String>,
}

impl SnapshotReferencedFiles {
    pub fn all_files(&self) -> Vec<String> {
        let mut files = vec![];
        for file in &self.segments {
            files.push(file.clone());
        }
        for file in &self.blocks {
            files.push(file.clone());
        }
        for file in &self.blocks_index {
            files.push(file.clone());
        }
        files
    }
}

// return all the segment\block\index files referenced by current snapshot.
#[async_backtrace::framed]
pub async fn get_snapshot_referenced_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Option<SnapshotReferencedFiles>> {
    // 1. Read the root snapshot.
    let root_snapshot_location_op = fuse_table.snapshot_loc().await?;
    if root_snapshot_location_op.is_none() {
        return Ok(None);
    }

    let root_snapshot_location = root_snapshot_location_op.unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let ver = TableMetaLocationGenerator::snapshot_version(root_snapshot_location.as_str());
    let params = LoadParams {
        location: root_snapshot_location.clone(),
        len_hint: None,
        ver,
        put_cache: true,
    };
    let root_snapshot = match reader.read(&params).await {
        Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
            // concurrent gc: someone else has already collected this snapshot, ignore it
            // warn!(
            //    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
            //    root_snapshot_location, self.table_info.desc, self.table_info.ident,
            //);
            return Ok(None);
        }
        Err(e) => return Err(e),
        Ok(v) => v,
    };

    let root_snapshot_lite = Arc::new(SnapshotLiteExtended {
        format_version: ver,
        snapshot_id: root_snapshot.snapshot_id,
        timestamp: root_snapshot.timestamp,
        segments: HashSet::from_iter(root_snapshot.segments.clone()),
        table_statistics_location: root_snapshot.table_statistics_location.clone(),
    });
    drop(root_snapshot);

    // 2. Find all segments referenced by the current snapshots
    let snapshots_io = SnapshotsIO::create(ctx.clone(), fuse_table.get_operator());
    let segments_opt = get_snapshot_referenced_segments(
        &snapshots_io,
        root_snapshot_location,
        root_snapshot_lite,
        |status| {
            ctx.set_status_info(&status);
        },
    )
    .await?;

    let segments_vec = match segments_opt {
        Some(segments) => segments,
        None => {
            return Ok(None);
        }
    };

    let locations_referenced = fuse_table
        .get_block_locations(ctx.clone(), &segments_vec, false)
        .await?;

    let mut segments = HashSet::with_capacity(segments_vec.len());
    segments_vec.into_iter().for_each(|(location, _)| {
        segments.insert(location);
    });
    Ok(Some(SnapshotReferencedFiles {
        segments,
        blocks: locations_referenced.block_location,
        blocks_index: locations_referenced.bloom_location,
    }))
}

// return orphan files to be purged
#[async_backtrace::framed]
async fn get_orphan_files_to_be_purged(
    fuse_table: &FuseTable,
    referenced_files: HashSet<String>,
    retention_time: DateTime<Utc>,
) -> Result<Vec<String>> {
    let files_to_be_purged = match referenced_files.iter().next().cloned() {
        Some(location) => {
            let prefix = SnapshotsIO::get_s3_prefix_from_file(&location);
            if let Some(prefix) = prefix {
                fuse_table
                    .list_files(prefix, |location, modified| {
                        modified <= retention_time && !referenced_files.contains(&location)
                    })
                    .await?
            } else {
                vec![]
            }
        }
        None => {
            vec![]
        }
    };

    Ok(files_to_be_purged)
}

#[async_backtrace::framed]
pub async fn do_gc_orphan_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
) -> Result<()> {
    // 1. Get all the files referenced by the current snapshot
    let referenced_files = match get_snapshot_referenced_files(fuse_table, ctx).await? {
        Some(referenced_files) => referenced_files,
        None => return Ok(()),
    };
    let status = format!(
        "gc orphan: read referenced files:{},{},{}, cost:{} sec",
        referenced_files.segments.len(),
        referenced_files.blocks.len(),
        referenced_files.blocks_index.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 2. Purge orphan segment files.
    // 2.1 Get orphan segment files to be purged
    let segment_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.segments, retention_time)
            .await?;
    let status = format!(
        "gc orphan: read segment_locations_to_be_purged:{}, cost:{} sec",
        segment_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 2.2 Delete all the orphan segment files to be purged
    let purged_file_num = segment_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files_and_cache::<CompactSegmentInfo, _, _>(
            ctx.clone(),
            HashSet::from_iter(segment_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan: purged segment files:{}, cost:{} sec",
        purged_file_num,
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 3. Purge orphan block files.
    // 3.1 Get orphan block files to be purged
    let block_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.blocks, retention_time).await?;
    let status = format!(
        "gc orphan: read block_locations_to_be_purged:{}, cost:{} sec",
        block_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 3.2 Delete all the orphan block files to be purged
    let purged_file_num = block_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files(
            ctx.clone(),
            HashSet::from_iter(block_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan: purged block files:{}, cost:{} sec",
        purged_file_num,
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 4. Purge orphan block index files.
    // 4.1 Get orphan block index files to be purged
    let index_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.blocks_index, retention_time)
            .await?;
    let status = format!(
        "gc orphan: read index_locations_to_be_purged:{}, cost:{} sec",
        index_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 4.2 Delete all the orphan block index files to be purged
    let purged_file_num = index_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files(
            ctx.clone(),
            HashSet::from_iter(index_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan: purged block index files:{}, cost:{} sec",
        purged_file_num,
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    Ok(())
}

#[async_backtrace::framed]
pub async fn do_dry_run_orphan_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
    purge_files: &mut Vec<String>,
    dry_run_limit: usize,
) -> Result<()> {
    // 1. Get all the files referenced by the current snapshot
    let referenced_files = match get_snapshot_referenced_files(fuse_table, ctx).await? {
        Some(referenced_files) => referenced_files,
        None => return Ok(()),
    };
    let status = format!(
        "dry_run orphan: read referenced files:{},{},{}, cost:{} sec",
        referenced_files.segments.len(),
        referenced_files.blocks.len(),
        referenced_files.blocks_index.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    // 2. Get purge orphan segment files.
    let segment_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.segments, retention_time)
            .await?;
    let status = format!(
        "dry_run orphan: read segment_locations_to_be_purged:{}, cost:{} sec",
        segment_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    purge_files.extend(segment_locations_to_be_purged);
    if purge_files.len() >= dry_run_limit {
        return Ok(());
    }

    // 3. Get purge orphan block files.
    let block_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.blocks, retention_time).await?;
    let status = format!(
        "dry_run orphan: read block_locations_to_be_purged:{}, cost:{} sec",
        block_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);
    purge_files.extend(block_locations_to_be_purged);
    if purge_files.len() >= dry_run_limit {
        return Ok(());
    }

    // 4. Get purge orphan block index files.
    let index_locations_to_be_purged =
        get_orphan_files_to_be_purged(fuse_table, referenced_files.blocks_index, retention_time)
            .await?;
    let status = format!(
        "dry_run orphan: read index_locations_to_be_purged:{}, cost:{} sec",
        index_locations_to_be_purged.len(),
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);

    purge_files.extend(index_locations_to_be_purged);

    Ok(())
}

#[async_backtrace::framed]
pub async fn do_vacuum(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<String>>> {
    let start = Instant::now();
    // First, do purge
    let instant = Some(NavigationPoint::TimePoint(retention_time));
    let purge_files_opt = fuse_table
        .purge(ctx.clone(), instant, true, dry_run_limit)
        .await?;
    let status = format!(
        "do_vacuum: purged table, cost:{} sec",
        start.elapsed().as_secs()
    );
    info!(status);
    ctx.set_status_info(&status);
    if let Some(mut purge_files) = purge_files_opt {
        let dry_run_limit = dry_run_limit.unwrap();
        if purge_files.len() >= dry_run_limit {
            return Ok(Some(purge_files));
        }

        do_dry_run_orphan_files(
            fuse_table,
            &ctx,
            retention_time,
            start,
            &mut purge_files,
            dry_run_limit,
        )
        .await?;
        Ok(Some(purge_files))
    } else {
        debug_assert!(dry_run_limit.is_none());
        do_gc_orphan_files(fuse_table, &ctx, retention_time, start).await?;
        Ok(None)
    }
}
