//  Copyright 2021 Datafuse Labs.
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

use std::marker::PhantomData;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_table_meta::meta::BlockFilter;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::SnapshotVersion;
use common_storages_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::constants::FUSE_TBL_BLOCK_PREFIX;
use crate::constants::FUSE_TBL_SEGMENT_PREFIX;
use crate::constants::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;
use crate::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;

static SNAPSHOT_V0: SnapshotVersion = SnapshotVersion::V0(PhantomData);
static SNAPSHOT_V1: SnapshotVersion = SnapshotVersion::V1(PhantomData);

#[derive(Clone)]
pub struct TableMetaLocationGenerator {
    prefix: String,
}

impl TableMetaLocationGenerator {
    pub fn with_prefix(prefix: String) -> Self {
        Self { prefix }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn gen_block_location(&self) -> (Location, Uuid) {
        let part_uuid = Uuid::new_v4();
        (
            (
                format!(
                    "{}/{}/{}_v{}.parquet",
                    &self.prefix,
                    FUSE_TBL_BLOCK_PREFIX,
                    part_uuid.as_simple(),
                    DataBlock::VERSION,
                ),
                DataBlock::VERSION,
            ),
            part_uuid,
        )
    }

    pub fn block_bloom_index_location(&self, block_id: &Uuid) -> Location {
        (
            format!(
                "{}/{}/{}_v{}.parquet",
                &self.prefix,
                FUSE_TBL_XOR_BLOOM_INDEX_PREFIX,
                block_id.as_simple(),
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    pub fn gen_segment_info_location(&self) -> String {
        let segment_uuid = Uuid::new_v4().simple().to_string();
        format!(
            "{}/{}/{}_v{}.json",
            &self.prefix,
            FUSE_TBL_SEGMENT_PREFIX,
            segment_uuid,
            SegmentInfo::VERSION,
        )
    }

    pub fn snapshot_location_from_uuid(&self, id: &Uuid, version: u64) -> Result<String> {
        let snapshot_version = SnapshotVersion::try_from(version)?;
        Ok(snapshot_version.create(id, &self.prefix))
    }

    pub fn snapshot_version(location: impl AsRef<str>) -> u64 {
        if location.as_ref().ends_with(SNAPSHOT_V1.suffix()) {
            SNAPSHOT_V1.version()
        } else {
            SNAPSHOT_V0.version()
        }
    }

    pub fn gen_last_snapshot_hint_location(&self) -> String {
        format!("{}/{}", &self.prefix, FUSE_TBL_LAST_SNAPSHOT_HINT)
    }
}

trait SnapshotLocationCreator {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String;
    fn suffix(&self) -> &'static str;
}

impl SnapshotLocationCreator for SnapshotVersion {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String {
        format!(
            "{}/{}/{}{}",
            prefix.as_ref(),
            FUSE_TBL_SNAPSHOT_PREFIX,
            id.simple(),
            self.suffix(),
        )
    }

    fn suffix(&self) -> &'static str {
        match self {
            SnapshotVersion::V0(_) => "",
            SnapshotVersion::V1(_) => "_v1.json",
        }
    }
}
