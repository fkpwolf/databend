// Copyright 2023 Datafuse Labs.
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
//

use std::marker::PhantomData;

use common_exception::Result;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use serde_json::from_slice;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SegmentInfoVersion;
use storages_common_table_meta::meta::SnapshotVersion;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use storages_common_table_meta::meta::TableSnapshotStatisticsVersion;

#[async_trait::async_trait]
pub trait VersionedReader<T> {
    async fn read<R>(&self, read: R) -> Result<T>
    where R: AsyncRead + Unpin + Send;
}

#[async_trait::async_trait]
impl VersionedReader<TableSnapshot> for SnapshotVersion {
    async fn read<R>(&self, reader: R) -> Result<TableSnapshot>
    where R: AsyncRead + Unpin + Send {
        let r = match self {
            SnapshotVersion::V2(v) => {
                let mut ts = load_by_version(reader, v).await?;
                ts.schema = TableSchema::init_if_need(ts.schema);
                ts
            }
            SnapshotVersion::V1(v) => load_by_version(reader, v).await?.into(),
            SnapshotVersion::V0(v) => load_by_version(reader, v).await?.into(),
        };
        Ok(r)
    }
}

#[async_trait::async_trait]
impl VersionedReader<TableSnapshotStatistics> for TableSnapshotStatisticsVersion {
    async fn read<R>(&self, reader: R) -> Result<TableSnapshotStatistics>
    where R: AsyncRead + Unpin + Send {
        let r = match self {
            TableSnapshotStatisticsVersion::V0(v) => load_by_version(reader, v).await?,
        };
        Ok(r)
    }
}

#[async_trait::async_trait]
impl VersionedReader<SegmentInfo> for (SegmentInfoVersion, TableSchemaRef) {
    async fn read<R>(&self, reader: R) -> Result<SegmentInfo>
    where R: AsyncRead + Unpin + Send {
        let schema = &self.1;
        let r = match &self.0 {
            SegmentInfoVersion::V2(v) => load_by_version(reader, v).await?,
            SegmentInfoVersion::V1(v) => {
                let data = load_by_version(reader, v).await?;
                let (_, fields) = schema.leaf_fields();
                SegmentInfo::from_v1(data, &fields)
            }
            SegmentInfoVersion::V0(v) => {
                let data = load_by_version(reader, v).await?;
                let (_, fields) = schema.leaf_fields();
                SegmentInfo::from_v0(data, &fields)
            }
        };
        Ok(r)
    }
}

async fn load_by_version<R, T>(mut reader: R, _v: &PhantomData<T>) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    let mut buffer: Vec<u8> = vec![];
    use futures::AsyncReadExt;
    reader.read_to_end(&mut buffer).await?;
    Ok(from_slice::<T>(&buffer)?)
}
