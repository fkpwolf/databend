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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_meta_app::schema::TableInfo;

use crate::extras::Extras;
use crate::extras::Statistics;
use crate::partition::Partitions;
use crate::plans::Projection;
use crate::stage_table::StageTableInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SourceInfo {
    // Normal table source, `fuse/system`.
    TableSource(TableInfo),

    // Internal/External source, like `s3://` or `azblob://`.
    StageSource(StageTableInfo),
}

impl SourceInfo {
    pub fn schema(&self) -> Arc<DataSchema> {
        match self {
            SourceInfo::TableSource(table_info) => table_info.schema(),
            SourceInfo::StageSource(table_info) => table_info.schema(),
        }
    }

    pub fn desc(&self) -> String {
        match self {
            SourceInfo::TableSource(table_info) => table_info.desc.clone(),
            SourceInfo::StageSource(table_info) => table_info.desc(),
        }
    }
}

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    // TODO catalog id is better
    pub catalog: String,
    pub source_info: SourceInfo,

    /// Required fields to scan.
    ///
    /// After optimization, only a sub set of the fields in `table_info.schema().fields` are needed.
    /// The key is the column_index of `ColumnEntry` in `Metadata`.
    ///
    /// If it is None, one should use `table_info.schema().fields()`.
    pub scan_fields: Option<BTreeMap<usize, DataField>>,

    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,

    pub tbl_args: Option<Vec<DataValue>>,
    pub push_downs: Option<Extras>,
}

impl ReadDataSourcePlan {
    /// Return schema after the projection
    pub fn schema(&self) -> DataSchemaRef {
        self.scan_fields
            .clone()
            .map(|x| {
                let fields: Vec<_> = x.iter().map(|(_, f)| f.clone()).collect();
                Arc::new(self.source_info.schema().project_by_fields(fields))
            })
            .unwrap_or_else(|| self.source_info.schema())
    }

    /// Return designated required fields or all fields in a hash map.
    pub fn scan_fields(&self) -> BTreeMap<usize, DataField> {
        self.scan_fields
            .clone()
            .unwrap_or_else(|| self.source_info.schema().fields_map())
    }

    pub fn projections(&self) -> Projection {
        let default_proj = || {
            (0..self.source_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        if let Some(Extras {
            projection: Some(prj),
            ..
        }) = &self.push_downs
        {
            prj.clone()
        } else {
            Projection::Columns(default_proj())
        }
    }
}
