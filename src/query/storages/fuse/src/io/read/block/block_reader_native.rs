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

use std::io::BufReader;
use std::ops::Range;
use std::time::Instant;

use common_arrow::arrow::array::Array;
use common_arrow::native::read::reader::NativeReader;
use common_arrow::native::read::NativeReadBuf;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Value;
use opendal::Object;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::metrics::metrics_inc_remote_io_read_bytes;
use crate::metrics::metrics_inc_remote_io_read_milliseconds;
use crate::metrics::metrics_inc_remote_io_read_parts;
use crate::metrics::metrics_inc_remote_io_seeks;

// Native storage format

pub trait NativeReaderExt: NativeReadBuf + std::io::Seek + Send + Sync {}
impl<T: NativeReadBuf + std::io::Seek + Send + Sync> NativeReaderExt for T {}

pub type Reader = Box<dyn NativeReaderExt>;

impl BlockReader {
    pub async fn async_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, NativeReader<Reader>)>> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.project_indices.len());

        for (index, (column_id, field, _)) in self.project_indices.iter() {
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                join_handlers.push(Self::read_native_columns_data(
                    self.operator.object(&part.location),
                    *index,
                    column_meta,
                    &part.range,
                    field.data_type().clone(),
                ));

                // Perf
                {
                    let (_, len) = column_meta.offset_length();
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }
            }
        }

        let start = Instant::now();
        let res = futures::future::try_join_all(join_handlers).await;

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        res
    }

    pub async fn read_native_columns_data(
        o: Object,
        index: usize,
        meta: &ColumnMeta,
        range: &Option<Range<usize>>,
        data_type: common_arrow::arrow::datatypes::DataType,
    ) -> Result<(usize, NativeReader<Reader>)> {
        use backon::ExponentialBackoff;
        use backon::Retryable;

        let (offset, length) = meta.offset_length();
        let mut meta = meta.as_native().unwrap().clone();

        if let Some(range) = range {
            meta = meta.slice(range.start, range.end);
        }

        let reader = { || async { o.range_read(offset..offset + length).await } }
            .retry(ExponentialBackoff::default())
            .when(|err| err.is_temporary())
            .await?;

        let reader: Reader = Box::new(std::io::Cursor::new(reader));
        let fuse_reader = NativeReader::new(reader, data_type, meta.pages.clone(), vec![]);
        Ok((index, fuse_reader))
    }

    pub fn sync_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, NativeReader<Reader>)>> {
        let part = FusePartInfo::from_part(&part)?;

        let mut results = Vec::with_capacity(self.project_indices.len());

        for (index, (column_id, field, _)) in self.project_indices.iter() {
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                let op = self.operator.clone();

                let location = part.location.clone();
                let result = Self::sync_read_native_column(
                    op.object(&location),
                    *index,
                    column_meta,
                    &part.range,
                    field.data_type().clone(),
                );

                results.push(result?);
            }
        }

        Ok(results)
    }

    pub fn sync_read_native_column(
        o: Object,
        index: usize,
        column_meta: &ColumnMeta,
        range: &Option<Range<usize>>,
        data_type: common_arrow::arrow::datatypes::DataType,
    ) -> Result<(usize, NativeReader<Reader>)> {
        let mut column_meta = column_meta.as_native().unwrap().clone();

        if let Some(range) = range {
            column_meta = column_meta.slice(range.start, range.end);
        }

        let (offset, length) = (
            column_meta.offset,
            column_meta.pages.iter().map(|p| p.length).sum::<u64>(),
        );
        let reader = o.blocking_range_reader(offset..offset + length)?;
        let reader: Reader = Box::new(BufReader::new(reader));
        let fuse_reader = NativeReader::new(reader, data_type, column_meta.pages, vec![]);
        Ok((index, fuse_reader))
    }

    pub fn build_block(&self, chunks: Vec<(usize, Box<dyn Array>)>) -> Result<DataBlock> {
        let mut entries = Vec::with_capacity(chunks.len());
        // they are already the leaf columns without inner
        // TODO support tuple in native storage
        let mut rows = 0;
        for (index, (_, _, f)) in self.project_indices.iter() {
            if let Some(array) = chunks.iter().find(|c| c.0 == *index).map(|c| c.1.clone()) {
                entries.push(BlockEntry {
                    data_type: f.clone(),
                    value: Value::Column(Column::from_arrow(array.as_ref(), f)),
                });
                rows = array.len();
            }
        }
        Ok(DataBlock::new(entries, rows))
    }
}
