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

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::Expression;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;
use serde_json::json;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::FuseTable;
use crate::Table;

pub struct ClusteringInformation<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub cluster_keys: Vec<Expression>,
}

struct ClusteringStatistics {
    total_block_count: u64,
    total_constant_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    block_depth_histogram: VariantValue,
}

impl Default for ClusteringStatistics {
    fn default() -> Self {
        ClusteringStatistics {
            total_block_count: 0,
            total_constant_block_count: 0,
            average_overlaps: 0.0,
            average_depth: 0.0,
            block_depth_histogram: VariantValue::from(json!({})),
        }
    }
}

impl<'a> ClusteringInformation<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        cluster_keys: Vec<Expression>,
    ) -> Self {
        Self {
            ctx,
            table,
            cluster_keys,
        }
    }

    pub async fn get_clustering_info(&self) -> Result<DataBlock> {
        let snapshot = self.table.read_table_snapshot().await?;

        let mut info = ClusteringStatistics::default();
        if let Some(snapshot) = snapshot {
            let segment_locations = &snapshot.segments;
            let segments_io = SegmentsIO::create(self.ctx.clone(), self.table.operator.clone());
            let segments = segments_io
                .read_segments(segment_locations)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            if !segments.is_empty() {
                let blocks = segments.iter().flat_map(|s| s.blocks.iter());
                info = self.get_clustering_stats(blocks)?
            }
        };

        let names = self
            .cluster_keys
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<String>>()
            .join(", ");
        let cluster_by_keys = format!("({})", names);

        Ok(DataBlock::create(ClusteringInformation::schema(), vec![
            Series::from_data(vec![cluster_by_keys]),
            Series::from_data(vec![info.total_block_count]),
            Series::from_data(vec![info.total_constant_block_count]),
            Series::from_data(vec![info.average_overlaps]),
            Series::from_data(vec![info.average_depth]),
            Series::from_data(vec![info.block_depth_histogram]),
        ]))
    }

    fn get_min_max_stats(&self, block: &BlockMeta) -> Result<(Vec<DataValue>, Vec<DataValue>)> {
        if self.table.cluster_keys(self.ctx.clone()) != self.cluster_keys
            || block.cluster_stats.is_none()
        {
            // Todo(zhyass): support manually specifying the cluster key.
            return Err(ErrorCode::Unimplemented("Unimplemented"));
        }

        let cluster_key_id = block.cluster_stats.clone().unwrap().cluster_key_id;
        let default_cluster_key_id = self.table.cluster_key_meta.clone().unwrap().0;
        if cluster_key_id != default_cluster_key_id {
            return Err(ErrorCode::Unimplemented("Unimplemented"));
        }

        let cluster_stats = block.cluster_stats.clone().unwrap();
        Ok((cluster_stats.min, cluster_stats.max))
    }

    fn get_clustering_stats<'b>(
        &self,
        blocks: impl Iterator<Item = &'b Arc<BlockMeta>>,
    ) -> Result<ClusteringStatistics> {
        // Gather all cluster statistics points to a sorted Map.
        // Key: The cluster statistics points.
        // Value: 0: The block indexes with key as min value;
        //        1: The block indexes with key as max value;
        let mut points_map: BTreeMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
        let mut total_constant_block_count = 0;
        let mut total_block_count = 0;
        for (i, block) in blocks.enumerate() {
            let (min, max) = self.get_min_max_stats(block.as_ref())?;
            if min.eq(&max) {
                total_constant_block_count += 1;
            }

            points_map
                .entry(min.clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));

            points_map
                .entry(max.clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            total_block_count += 1;
        }

        // calculate overlaps and depth.
        let mut statis = Vec::new();
        // key: the block index.
        // value: (overlaps, depth).
        let mut unfinished_parts: HashMap<usize, (usize, usize)> = HashMap::new();
        for (start, end) in points_map.values() {
            let point_depth = unfinished_parts.len() + start.len();

            for (_, val) in unfinished_parts.iter_mut() {
                val.0 += start.len();
                val.1 = cmp::max(val.1, point_depth);
            }

            start.iter().for_each(|&idx| {
                unfinished_parts.insert(idx, (point_depth - 1, point_depth));
            });

            end.iter().for_each(|&idx| {
                let stat = unfinished_parts.remove(&idx).unwrap();
                statis.push(stat);
            });
        }
        assert_eq!(unfinished_parts.len(), 0);

        let mut sum_overlap = 0;
        let mut sum_depth = 0;
        let length = statis.len();
        let mp = statis
            .into_iter()
            .fold(BTreeMap::new(), |mut acc, (overlap, depth)| {
                sum_overlap += overlap;
                sum_depth += depth;

                let bucket = get_buckets(depth);
                acc.entry(bucket).and_modify(|v| *v += 1).or_insert(1u32);
                acc
            });
        // round the float to 4 decimal places.
        let average_depth = (10000.0 * sum_depth as f64 / length as f64).round() / 10000.0;
        let average_overlaps = (10000.0 * sum_overlap as f64 / length as f64).round() / 10000.0;

        let objects = mp.iter().fold(
            serde_json::Map::with_capacity(mp.len()),
            |mut acc, (bucket, count)| {
                acc.insert(format!("{:05}", bucket), json!(count));
                acc
            },
        );
        let block_depth_histogram = VariantValue::from(serde_json::Value::Object(objects));

        Ok(ClusteringStatistics {
            total_block_count,
            total_constant_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        })
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("cluster_by_keys", Vu8::to_data_type()),
            DataField::new("total_block_count", u64::to_data_type()),
            DataField::new("total_constant_block_count", u64::to_data_type()),
            DataField::new("average_overlaps", f64::to_data_type()),
            DataField::new("average_depth", f64::to_data_type()),
            DataField::new("block_depth_histogram", VariantArrayType::new_impl()),
        ])
    }
}

// The histogram contains buckets with widths:
// 1 to 16 with increments of 1.
// For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …).
// e.g. If val is 2, the bucket is 2. If val is 18, the bucket is 32.
fn get_buckets(val: usize) -> u32 {
    let mut val = val as u32;
    if val <= 16 || val & (val - 1) == 0 {
        return val;
    }

    val |= val >> 1;
    val |= val >> 2;
    val |= val >> 4;
    val |= val >> 8;
    val |= val >> 16;
    val + 1
}
