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

use std::marker::PhantomData;

use common_exception::Result;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::SegmentInfoVersion;
use common_storages_table_meta::meta::SnapshotVersion;
use common_storages_table_meta::meta::TableSnapshot;
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use serde_json::from_slice;

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
            SnapshotVersion::V1(v) => load_by_version(reader, v).await?,
            SnapshotVersion::V0(v) => load_by_version(reader, v).await?.into(),
        };
        Ok(r)
    }
}

#[async_trait::async_trait]
impl VersionedReader<SegmentInfo> for SegmentInfoVersion {
    async fn read<R>(&self, reader: R) -> Result<SegmentInfo>
    where R: AsyncRead + Unpin + Send {
        let r = match self {
            SegmentInfoVersion::V1(v) => load_by_version(reader, v).await?,
            SegmentInfoVersion::V0(v) => load_by_version(reader, v).await?.into(),
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
