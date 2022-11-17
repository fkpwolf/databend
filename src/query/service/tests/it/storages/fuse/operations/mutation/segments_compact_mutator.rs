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

use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_mutator::TableMutator;
use common_datablocks::DataBlock;
use common_datablocks::SendableDataBlockStream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::DataOperator;
use common_storages_fuse::io::BlockWriter;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::SegmentInfoReader;
use common_storages_fuse::io::SegmentWriter;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::operations::CompactOptions;
use common_storages_fuse::operations::SegmentCompactMutator;
use common_storages_fuse::operations::SegmentCompactionState;
use common_storages_fuse::operations::SegmentCompactor;
use common_storages_fuse::statistics::gen_columns_statistics;
use common_storages_fuse::statistics::reducers::merge_statistics_mut;
use common_storages_fuse::statistics::BlockStatistics;
use common_storages_fuse::statistics::StatisticsAccumulator;
use common_storages_fuse::FuseTable;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use common_storages_table_meta::meta::Versioned;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use futures_util::TryStreamExt;
use rand::thread_rng;
use rand::Rng;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_compact_segment_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let qry = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), qry).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();
    mutator.try_commit(table.clone()).await?;

    // check segment count
    let qry = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), qry).await?;
    // after compact, in our case, there should be only 1 segment left
    assert_eq!(1, check_count(stream).await?);

    // check block count
    let qry = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);
    Ok(())
}

#[tokio::test]
async fn test_compact_segment_resolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    // before commit compact segments, gives 9 append commits
    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    mutator.try_commit(table.clone()).await?;

    // check segment count
    let count_seg = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), count_seg).await?;
    // after compact, in our case, there should be only 1 + num_inserts segments left
    // during compact retry, newly appended segments will NOT be compacted again
    assert_eq!(1 + num_inserts as u64, check_count(stream).await?);

    // check block count
    let count_block = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), count_block).await?;
    assert_eq!(num_inserts as u64 * 2, check_count(stream).await?);

    // check table statistics

    let latest = table.refresh(ctx.as_ref()).await?;
    let latest_fuse_table = FuseTable::try_from_table(latest.as_ref())?;
    let table_statistics = latest_fuse_table.table_statistics()?.unwrap();

    assert_eq!(table_statistics.num_rows.unwrap() as usize, num_inserts * 2);

    Ok(())
}

#[tokio::test]
async fn test_compact_segment_unresolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);

    // try compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    {
        // inject a unresolvable commit
        compact_segment(ctx.clone(), &table).await?;
    }

    // the compact operation committed latter should failed
    let r = mutator.try_commit(table.clone()).await;
    assert!(r.is_err());
    assert_eq!(r.err().unwrap().code(), ErrorCode::STORAGE_OTHER);

    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    let qry = "insert into t values(1)";
    for _ in 0..n {
        execute_command(ctx.clone(), qry).await?;
    }
    Ok(())
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    blocks[0].column(0).get_u64(0)
}

async fn compact_segment(ctx: Arc<QueryContext>, table: &Arc<dyn Table>) -> Result<()> {
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?.unwrap();
    mutator.try_commit(table.clone()).await
}

async fn build_mutator(
    tbl: &FuseTable,
    ctx: Arc<dyn TableContext>,
    limit: Option<usize>,
) -> Result<Option<Box<dyn TableMutator>>> {
    let snapshot_opt = tbl.read_table_snapshot().await?;
    let base_snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot, no compaction.
        return Ok(None);
    };

    if base_snapshot.summary.block_count <= 1 {
        return Ok(None);
    }

    let block_per_seg = tbl.get_option("block_per_segment", 1000);

    let compact_params = CompactOptions {
        base_snapshot,
        block_per_seg,
        limit,
    };

    let mut segment_mutator = SegmentCompactMutator::try_create(
        ctx.clone(),
        compact_params,
        tbl.meta_location_generator().clone(),
        tbl.get_operator(),
    )?;

    if segment_mutator.target_select().await? {
        Ok(Some(Box::new(segment_mutator)))
    } else {
        Ok(None)
    }
}

#[tokio::test]
async fn test_segment_compactor() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    {
        let case_name = "highly fragmented segments";
        let threshold = 10;
        let case = CompactCase {
            // 3 fragmented segments
            // - each of them have number blocks lesser than `threshold`
            blocks_number_of_input_segments: vec![1, 2, 3],
            // - these segments should be compacted into one
            expected_number_of_output_segments: 1,
            // - which contains 6 blocks
            expected_block_number_of_new_segments: vec![1 + 2 + 3],
            case_name,
        };

        // run, and verify that
        // - numbers are as expected
        //   - number of the newly created segments (which are compacted)
        //   - number of segments unchanged
        // - other general invariants
        //   - unchanged segments still be there
        //   - blocks and the order of them are not changed
        //   - statistics are as expected
        //   - the output segments could not be compacted further
        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        let case_name = "greedy compact, but not too greedy(1), right assoc";
        let threshold = 10;
        let case = CompactCase {
            // - 4 segments
            blocks_number_of_input_segments: vec![1, 8, 2, 8],
            // - these segments should be compacted into 2 new segments
            expected_number_of_output_segments: 2,
            // compaction is right-assoc
            // -  (2 + 8) meets threshold 10
            //    they should be compacted into one new segment.
            //    although the next segment contains only 1 segment, it should NOT
            //    be compacted (the not too greedy rule)
            // - (1 + 8) should be compacted into another new segment
            //
            // To let the case more readable, we specify the block numbers of new
            // segments in the order of input segments
            expected_block_number_of_new_segments: vec![1 + 8, 2 + 8],
            case_name,
        };
        // run & verify
        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        let case_name = "greedy compact, but not too greedy (2), right-assoc";
        let threshold = 10;
        let case = CompactCase {
            // 4 segments
            blocks_number_of_input_segments: vec![5, 2, 3, 6],
            // these segments should be compacted into 2 segments
            expected_number_of_output_segments: 2,
            // (2 + 3 + 6) exceeds the threshold 10
            //  - but not too much, lesser than 2 * threshold, which is 20;
            //  - they are allowed to be compacted into one, to avoid the ripple effects:
            //      since the order of blocks should be preserved, they might be cases, that
            //      to compact one fragment, a large amount of non-fragmented segments have to be
            //      split into pieces and re-compacted.
            // - but the last segment of 5 blocks should be kept alone
            //   thus, only one new segment will be generated (the not too greedy rule)
            //   if it is the last segment of the snapshot, we just tolerant this situation.
            //   if a limited
            expected_block_number_of_new_segments: vec![2 + 3 + 6],
            case_name,
        };

        // run & verify
        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(1), right-assoc";
        let threshold = 10;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![5, 6, 11, 2, 10],
            // these segments should be compacted into 2 new segments, 1 segment unchanged
            // unchanged: (10)
            // new segments: (11 + 2), ( 5 + 6)
            expected_number_of_output_segments: 2 + 1,
            expected_block_number_of_new_segments: vec![5 + 6, 11 + 2],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(2)";
        let threshold = 10;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![10, 10, 1, 2, 10],
            // these segments should be compacted into
            // (10), (10), (1 + 2 + 10)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![(1 + 2 + 10)],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(3)";
        let threshold = 10;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 19, 5, 6],
            // these segments should be compacted into
            // (1), (19), (5, 6)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![(5 + 6)],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        // edge case: empty segments should be dropped

        let case_name = "empty segments should be dropped";
        let threshold = 10;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![0, 1, 0, 19, 0, 5, 0, 6, 0],
            // these segments should be compacted into
            // (1), (19), (5, 6)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![(5 + 6)],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        // edge case: single jumbo block

        let case_name = "single jumbo block";
        let threshold = 3;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![10],
            expected_number_of_output_segments: 1,
            expected_block_number_of_new_segments: vec![],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        let case_name = "jumbo block with single fragment";
        let threshold = 3;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![7, 2],
            // inputs will not be compacted ( 7 > 2 * 3)
            expected_number_of_output_segments: 2,
            // no new segment should be generated
            expected_block_number_of_new_segments: vec![],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        let case_name = "right assoc";
        let threshold = 10;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![8, 5, 7],
            expected_number_of_output_segments: 2,
            // one new segment should be generated, since
            // - (5 + 7) < 2 * 10
            // - but (8 + 5 + 7) = 20 >= 20, which exceed the upper limit
            expected_block_number_of_new_segments: vec![5 + 7],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, None).await?;
    }

    {
        let case_name = "limit (normal case)";
        let threshold = 5;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 2, 3, 2, 3],
            expected_number_of_output_segments: 4,
            expected_block_number_of_new_segments: vec![2 + 3],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, Some(2)).await?;
    }

    {
        let case_name = "limit (auto adjust limit)";
        let threshold = 5;
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 2, 3, 2, 3],
            expected_number_of_output_segments: 4,
            expected_block_number_of_new_segments: vec![2 + 3],
            case_name,
        };

        // if limit is specified as 1, it will be adjust to 2 during execution
        // since at least two fragmented segments are needed for compaction
        let limit = Some(1);
        case.run_and_verify(&ctx, threshold, limit).await?;
    }

    {
        let case_name = "limit (abundant limit)";
        let threshold = 5;
        let limit = Some(5);
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 1, 3, 2, 3],
            expected_number_of_output_segments: 2,
            expected_block_number_of_new_segments: vec![1 + 1 + 3, 2 + 3],
            case_name,
        };

        case.run_and_verify(&ctx, threshold, limit).await?;
    }

    {
        // case: rand test

        let case_name = "rand";
        let threshold = 3;
        let mut rng = thread_rng();

        // let rounds = 200; // use this setting at home
        let rounds = 20;
        for _ in 0..rounds {
            let num_segments: usize = rng.gen_range(0..10);
            let mut blocks_number_of_input_segments = Vec::with_capacity(num_segments);

            // simulate the compaction process, verifies that the test target works as expected
            let mut num_accumulated_blocks = 0;
            let mut fragmented_segments = 0;

            // - number of segment expected in the output of compaction, includes both the compacted
            //   new segments and the unchanged non-fragmented segments
            let mut expected_number_of_output_segments = 0;
            // - number of new segments created during the compaction
            let mut expected_block_number_of_new_segments = vec![];
            for _ in 0..num_segments {
                let block_num: usize = rng.gen_range(0..20);
                blocks_number_of_input_segments.push(block_num);
            }

            // traverse the input segments in reversed order (to let the compaction "right-assoc")
            for item in blocks_number_of_input_segments.iter().rev() {
                let block_num = *item;
                if block_num != 0 {
                    let s = block_num + num_accumulated_blocks;
                    if s < threshold {
                        // input segment is fragmented, but fragments collected so far
                        // are not enough yet.
                        num_accumulated_blocks = s;
                        // only in this branch, the number of fragmented_segments will increase
                        fragmented_segments += 1;
                    } else if s >= threshold && s < 2 * threshold {
                        // input segment is fragmented, and fragments collected are
                        // large enough to be compacted
                        num_accumulated_blocks = 0;
                        // mark that a segment will be included in the output
                        expected_number_of_output_segments += 1;
                        if fragmented_segments > 0 {
                            // mark that a NEW segment will be generated, which
                            // "contains" all the fragmented segments collected so far.
                            expected_block_number_of_new_segments.push(s);
                        }
                        // reset state
                        fragmented_segments = 0;
                    } else {
                        // input segment is larger than threshold
                        // - fragmented segments collected so far should be compacted first
                        if fragmented_segments > 0 {
                            // some fragments left there, check them out
                            if fragmented_segments > 1 {
                                // if there are more than one fragments, a new segment is expected
                                // to be generated
                                expected_block_number_of_new_segments.push(num_accumulated_blocks);
                            }
                            // mark that another segment will be include in the output
                            expected_number_of_output_segments += 1;
                        }
                        // - after compacting the fragments, count this large segment in
                        expected_number_of_output_segments += 1;

                        // no fragments left currently, reset the counters
                        fragmented_segments = 0;
                        num_accumulated_blocks = 0;
                    }
                }
            }

            // finalize, compact left fragments if any
            if fragmented_segments > 0 {
                if fragmented_segments > 1 {
                    // if there are more than one fragments left there, a new segment should be created
                    expected_block_number_of_new_segments.push(num_accumulated_blocks);
                }
                // mark that another segment will be include in the output
                expected_number_of_output_segments += 1;
            }

            // To make the non-random test cases more readable, paths of newly created segments are
            // specified in the order of original(Input) order.
            // But during this simulated compaction, the paths are kept in reversed order,
            // so here we reverse the paths, to make the test verifications followed pass.
            expected_block_number_of_new_segments.reverse();
            let case = CompactCase {
                blocks_number_of_input_segments,
                expected_number_of_output_segments,
                expected_block_number_of_new_segments,
                case_name,
            };

            case.run_and_verify(&ctx, threshold as u64, None).await?;
        }
    }

    Ok(())
}

struct CompactSegmentTestFixture {
    threshold: u64,
    data_accessor: DataOperator,
    location_gen: TableMetaLocationGenerator,
    input_segments: Vec<Arc<SegmentInfo>>,
    input_segment_locations: Vec<Location>,
    // blocks of input_segments, order by segment
    input_blocks: Vec<BlockMeta>,
}

impl CompactSegmentTestFixture {
    fn try_new(ctx: &Arc<QueryContext>, block_per_seg: u64) -> Result<Self> {
        let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());
        let data_accessor = ctx.get_data_operator()?;
        Ok(Self {
            threshold: block_per_seg,
            data_accessor,
            location_gen,
            input_segments: vec![],
            input_segment_locations: vec![],
            input_blocks: vec![],
        })
    }

    async fn run<'a>(
        &'a mut self,
        num_block_of_segments: &'a [usize],
        limit: Option<usize>,
    ) -> Result<SegmentCompactionState> {
        let block_per_seg = self.threshold;
        let data_accessor = &self.data_accessor;
        let location_gen = &self.location_gen;
        let block_writer = BlockWriter::new(data_accessor, location_gen);

        let segment_writer = SegmentWriter::new(data_accessor, location_gen, &None);
        let seg_acc = SegmentCompactor::new(block_per_seg, segment_writer.clone());

        let (segments, locations, blocks) =
            Self::gen_segments(&block_writer, &segment_writer, num_block_of_segments).await?;
        self.input_segments = segments;
        self.input_segment_locations = locations;
        self.input_blocks = blocks;
        let limit = limit.unwrap_or(usize::MAX);
        seg_acc
            .compact(&self.input_segments, &self.input_segment_locations, limit)
            .await
    }

    async fn gen_segments(
        block_writer: &BlockWriter<'_>,
        segment_writer: &SegmentWriter<'_>,
        block_num_of_segments: &[usize],
    ) -> Result<(Vec<Arc<SegmentInfo>>, Vec<Location>, Vec<BlockMeta>)> {
        let mut segments = vec![];
        let mut locations = vec![];
        let mut collected_blocks = vec![];
        for num_blocks in block_num_of_segments {
            let blocks = TestFixture::gen_sample_blocks_ex(*num_blocks, 1, 1);
            let mut stats_acc = StatisticsAccumulator::default();
            for block in blocks {
                let block = block?;
                let col_stats = gen_columns_statistics(&block)?;

                let mut block_statistics = BlockStatistics::from(&block, "".to_owned(), None)?;
                let block_meta = block_writer.write(block, col_stats, None).await?;
                block_statistics.block_file_location = block_meta.location.0.clone();

                collected_blocks.push(block_meta.clone());
                stats_acc.add_with_block_meta(block_meta, block_statistics)?;
            }
            let col_stats = stats_acc.summary()?;
            let segment_info = SegmentInfo::new(stats_acc.blocks_metas, Statistics {
                row_count: stats_acc.summary_row_count,
                block_count: stats_acc.summary_block_count,
                perfect_block_count: stats_acc.perfect_block_count,
                uncompressed_byte_size: stats_acc.in_memory_size,
                compressed_byte_size: stats_acc.file_size,
                index_size: stats_acc.index_size,
                col_stats,
            });
            let location = segment_writer.write_segment_no_cache(&segment_info).await?;
            segments.push(Arc::new(segment_info));
            locations.push(location);
        }

        Ok((segments, locations, collected_blocks))
    }

    // verify that newly generated segments contain the proper number of blocks
    pub async fn verify_new_segments(
        case_name: &str,
        new_segment_paths: &[String],
        expected_num_blocks: &[usize],
        segment_reader: &SegmentInfoReader,
    ) -> Result<()> {
        // traverse the paths of new segments  in reversed order
        for (idx, x) in new_segment_paths.iter().rev().enumerate() {
            let seg = segment_reader.read(x, None, SegmentInfo::VERSION).await?;
            assert_eq!(
                seg.blocks.len(),
                expected_num_blocks[idx],
                "case name :{}, verify_block_number_of_new_segments",
                case_name
            );
        }
        Ok(())
    }
}

struct CompactCase {
    blocks_number_of_input_segments: Vec<usize>,
    expected_block_number_of_new_segments: Vec<usize>,
    // number of output segments, newly created and unchanged
    expected_number_of_output_segments: usize,
    case_name: &'static str,
}

impl CompactCase {
    async fn run_and_verify(
        &self,
        ctx: &Arc<QueryContext>,
        block_per_segment: u64,
        limit: Option<usize>,
    ) -> Result<()> {
        // setup & run
        let segment_reader = MetaReaders::segment_info_reader(ctx.get_data_operator()?.operator());
        let mut case_fixture = CompactSegmentTestFixture::try_new(ctx, block_per_segment)?;
        let r = case_fixture
            .run(&self.blocks_number_of_input_segments, limit)
            .await?;

        // verify that:

        // 1. number of newly generated segment is as expected
        let expected_num_of_new_segments = self.expected_block_number_of_new_segments.len();
        assert_eq!(
            r.new_segment_paths.len(),
            expected_num_of_new_segments,
            "case: {}, step: verify number of new segments generated, segment block size {:?}",
            self.case_name,
            self.blocks_number_of_input_segments,
        );

        // 2. number of segments is as expected (including both of the segments that not changed and newly generated segments)
        assert_eq!(
            r.segments_locations.len(),
            self.expected_number_of_output_segments,
            "case: {}, step: verify number of output segments (new segments and unchanged segments)",
            self.case_name,
        );

        // 3. each new segment contains expected number of blocks
        CompactSegmentTestFixture::verify_new_segments(
            self.case_name,
            &r.new_segment_paths,
            &self.expected_block_number_of_new_segments,
            &segment_reader,
        )
        .await?;

        // invariants 4 - 6 are general rules, for all the cases.
        let mut idx = 0;
        let mut statistics_of_input_segments = Statistics::default();
        let mut block_num_of_output_segments = vec![];

        // 4. input blocks should be there and in the original order
        // for location in r.segments_locations.iter().rev() {
        for location in r.segments_locations.iter() {
            let segment = segment_reader.read(&location.0, None, location.1).await?;
            merge_statistics_mut(&mut statistics_of_input_segments, &segment.summary)?;
            block_num_of_output_segments.push(segment.blocks.len());

            for x in &segment.blocks {
                let original_block_meta = &case_fixture.input_blocks[idx];
                assert_eq!(
                    original_block_meta,
                    x.as_ref(),
                    "case : {}, verify block order",
                    self.case_name
                );
                idx += 1;
            }
        }

        // 5. statistics should be the same
        assert_eq!(
            statistics_of_input_segments, r.statistics,
            "case : {}",
            self.case_name
        );

        // 6. the output segments can not be compacted further, if (no limit)
        if limit.is_none() {
            let mut case_fixture = CompactSegmentTestFixture::try_new(ctx, block_per_segment)?;
            let r = case_fixture
                .run(&block_num_of_output_segments, None)
                .await?;
            assert_eq!(
                r.new_segment_paths.len(),
                0,
                "case: {}, verify number of new segment",
                self.case_name
            );
            let num_of_output_segments = block_num_of_output_segments.len();
            assert_eq!(
                r.segments_locations.len(),
                num_of_output_segments,
                "case: {}, verify number of segments",
                self.case_name
            );
        }

        Ok(())
    }
}
