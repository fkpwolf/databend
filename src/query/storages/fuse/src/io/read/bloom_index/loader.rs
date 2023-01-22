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

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use futures_util::future::try_join_all;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::caches::BloomIndexMeta;
use storages_common_table_meta::meta::BlockFilter;
use storages_common_table_meta::meta::ColumnId;
use tracing::Instrument;

use crate::io::read::bloom_index::column_reader::BloomIndexColumnReader;
use crate::io::MetaReaders;
use crate::metrics::metrics_inc_block_index_read_bytes;
use crate::metrics::metrics_inc_block_index_read_milliseconds;
use crate::metrics::metrics_inc_block_index_read_nums;

/// load index column data
#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_bloom_filter_by_columns(
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    column_needed: &[String],
    path: &str,
    length: u64,
) -> Result<BlockFilter> {
    let bloom_index_meta = load_index_meta(dal.clone(), path, length).await?;
    let file_meta = &bloom_index_meta.0;
    if file_meta.row_groups.len() != 1 {
        return Err(ErrorCode::StorageOther(format!(
            "invalid v1 bloom index filter index, number of row group should be 1, but found {} row groups",
            file_meta.row_groups.len()
        )));
    }
    let row_group = &file_meta.row_groups[0];

    let fields = column_needed
        .iter()
        .map(|name| TableField::new(name, TableDataType::String))
        .collect::<Vec<_>>();

    let filter_schema = Arc::new(TableSchema::new(fields));

    // 1. load column data, as bytes
    let futs = column_needed
        .iter()
        .map(|col_name| load_column_bytes(&ctx, file_meta, col_name, path, &dal))
        .collect::<Vec<_>>();

    let start = Instant::now();

    let cols_data = try_join_all(futs)
        .instrument(tracing::debug_span!("join_columns"))
        .await?;

    // Perf.
    {
        metrics_inc_block_index_read_nums(cols_data.len() as u64);
        metrics_inc_block_index_read_milliseconds(start.elapsed().as_millis() as u64);
    }

    let column_descriptors = file_meta.schema_descr.columns();
    let arrow_schema = infer_schema(file_meta)?;
    let mut columns_array_iter = Vec::with_capacity(cols_data.len());
    let num_values = row_group.num_rows();

    // 2. deserialize column data

    // wrapping around Arc<Vec<u8>>, so that bytes clone can be avoided
    // later in the construction of PageReader
    struct Wrap(Arc<Vec<u8>>);
    impl AsRef<[u8]> for Wrap {
        #[inline]
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }
    let columns = row_group.columns();
    tracing::debug_span!("build_array_iter").in_scope(|| {
        for (bytes, col_idx) in cols_data.into_iter() {
            // Perf.
            {
                metrics_inc_block_index_read_bytes(bytes.len() as u64);
            }

            let compression_codec = columns[0]
                .column_chunk()
                .meta_data
                .as_ref()
                .ok_or_else(|| {
                    ErrorCode::Internal(format!("column meta is none, idx {}", col_idx))
                })?
                .codec;

            // TODO(xuanwo): return a understandable error code to user
            let compression = Compression::try_from(compression_codec).map_err(|e| {
                ErrorCode::Internal(format!("unrecognized compression: {} ", e))
            })?;
            let descriptor = file_meta.schema_descr.columns()[col_idx].descriptor.clone();

            let page_meta_data = PageMetaData {
                column_start: 0,
                num_values: num_values as i64,
                compression,
                descriptor,
            };

            let wrapped = Wrap(bytes);
            let page_reader = PageReader::new_with_page_meta(
                std::io::Cursor::new(wrapped), // we can not use &[u8] as Reader here, lifetime not valid
                page_meta_data,
                Arc::new(|_, _| true),
                vec![],
                usize::MAX,
            );
            let decompressor = BasicDecompressor::new(page_reader, vec![]);
            let decompressors = vec![decompressor];
            let types = vec![&column_descriptors[col_idx].descriptor.primitive_type];
            let field = arrow_schema.fields[col_idx].clone();
            let arrays = tracing::debug_span!("iter_to_arrays").in_scope(|| {
                column_iter_to_arrays(decompressors, types, field, Some(num_values), num_values)
            })?;
            columns_array_iter.push(arrays);
        }
        Ok::<_, ErrorCode>(())
    })?;

    let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_values, None);
    let next = tracing::debug_span!("deserializer_next").in_scope(|| deserializer.next());

    match next {
        None => Err(ErrorCode::Internal(
            "deserialize row group: fail to get a chunk",
        )),
        Some(Err(cause)) => Err(ErrorCode::from(cause)),
        Some(Ok(chunk)) => {
            let span = tracing::info_span!("from_chunk");
            let filter_block =
                span.in_scope(|| DataBlock::from_arrow_chunk(&chunk, &(&filter_schema).into()))?;
            Ok(BlockFilter {
                filter_schema,
                filter_block,
            })
        }
    }
}

/// Loads bytes and index of the given column.
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_column_bytes(
    _ctx: &Arc<dyn TableContext>, // TODO funny lifetime compile error, if this commented out
    file_meta: &FileMetaData,
    col_name: &str,
    path: &str,
    dal: &Operator,
) -> Result<(Arc<Vec<u8>>, usize)> {
    let storage_runtime = GlobalIORuntime::instance();
    let cols = file_meta.row_groups[0].columns();
    if let Some((idx, col_meta)) = cols
        .iter()
        .enumerate()
        .find(|(_, c)| c.descriptor().path_in_schema[0] == col_name)
    {
        let bytes = {
            let column_data_reader = BloomIndexColumnReader::new(
                path.to_owned(),
                idx as ColumnId,
                col_meta,
                dal.clone(),
            );
            async move { column_data_reader.read().await }
        }
        .execute_in_runtime(&storage_runtime)
        .await??;
        Ok((bytes, idx))
    } else {
        Err(ErrorCode::Internal(format!(
            "failed to find bloom index column. no such column {col_name}"
        )))
    }
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_index_meta(dal: Operator, path: &str, length: u64) -> Result<Arc<BloomIndexMeta>> {
    let storage_runtime = GlobalIORuntime::instance();
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::file_meta_data_reader(dal);
        // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
        // so we just pass a zero to reader
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: Some(length),
            ver: version,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&storage_runtime)
    .await?
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_index_column_data_from_storage(
    col_meta: ColumnChunkMetaData,
    dal: Operator,
    path: String,
) -> Result<Vec<u8>> {
    let chunk_meta = col_meta.metadata();
    let chunk_offset = chunk_meta.data_page_offset as u64;
    let col_len = chunk_meta.total_compressed_size as u64;
    let column_reader = dal.object(&path);
    let bytes = column_reader
        .range_read(chunk_offset..chunk_offset + col_len)
        .await?;
    Ok(bytes)
}

#[async_trait::async_trait]
trait InRuntime
where Self: Future
{
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<Self::Output>;
}

#[async_trait::async_trait]
impl<T> InRuntime for T
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
        runtime
            .try_spawn(self)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}
