// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Compression;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FusePartInfo {
    pub location: String,
    /// FusePartInfo itself is not versioned
    /// the `format_version` is the version of the block which the `location` points to
    pub format_version: u64,
    pub nums_rows: usize,
    pub columns_meta: HashMap<usize, ColumnMeta>,
    pub compression: Compression,

    /// page range in the file
    pub range: Option<Range<usize>>,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FusePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl FusePartInfo {
    pub fn create(
        location: String,
        format_version: u64,
        rows_count: u64,
        columns_meta: HashMap<usize, ColumnMeta>,
        compression: Compression,
        range: Option<Range<usize>>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FusePartInfo {
            location,
            format_version,
            columns_meta,
            nums_rows: rows_count as usize,
            compression,
            range,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FusePartInfo> {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to FusePartInfo.",
            )),
        }
    }
}
