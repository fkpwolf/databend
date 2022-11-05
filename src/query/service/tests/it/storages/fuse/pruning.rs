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

use std::sync::Arc;

use common_ast::ast::Engine;
use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planner::extras::Extras;
use common_sql::executor::add;
use common_sql::executor::col;
use common_sql::executor::lit;
use common_sql::executor::sub;
use common_sql::executor::ExpressionOp;
use common_storages_fuse::FuseTable;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::TableSnapshot;
use common_storages_table_meta::table::OPT_KEY_DATABASE_ID;
use common_storages_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_query::interpreters::CreateTableInterpreterV2;
use databend_query::interpreters::Interpreter;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::create_table_v2::CreateTablePlanV2;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::storages::fuse::pruning::BlockPruner;
use databend_query::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_query::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use opendal::Operator;

use crate::storages::fuse::table_test_fixture::TestFixture;

async fn apply_block_pruning(
    table_snapshot: Arc<TableSnapshot>,
    schema: DataSchemaRef,
    push_down: &Option<Extras>,
    ctx: Arc<QueryContext>,
    op: Operator,
) -> Result<Vec<Arc<BlockMeta>>> {
    let ctx: Arc<dyn TableContext> = ctx;
    let segment_locs = table_snapshot.segments.clone();
    BlockPruner::prune(&ctx, op, schema, push_down, segment_locs)
        .await
        .map(|v| v.into_iter().map(|(_, v)| v).collect())
}

#[tokio::test]
async fn test_block_pruner() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let test_tbl_name = "test_index_helper";
    let test_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);

    let num_blocks = 10;
    let row_per_block = 10;
    let num_blocks_opt = row_per_block.to_string();

    // create test table
    let create_table_plan = CreateTablePlanV2 {
        catalog: "default".to_owned(),
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        schema: test_schema.clone(),
        engine: Engine::Fuse,
        storage_params: None,
        options: [
            (FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
            (FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_default_exprs: vec![],
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
    };

    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // get table
    let catalog = ctx.get_catalog("default")?;
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let gen_col =
        |value, rows| Series::from_data(std::iter::repeat(value).take(rows).collect::<Vec<u64>>());

    // prepare test blocks
    // - there will be `num_blocks` blocks, for each block, it comprises of `row_per_block` rows,
    //    in our case, there will be 10 blocks, and 10 rows for each block
    let blocks = (0..num_blocks)
        .into_iter()
        .map(|idx| {
            DataBlock::create(test_schema.clone(), vec![
                // value of column a always equals  1
                gen_col(1, row_per_block),
                // for column b
                // - for all block `B` in blocks, whose index is `i`
                // - for all row in `B`, value of column b  equals `i`
                gen_col(idx as u64, row_per_block),
            ])
        })
        .collect::<Vec<_>>();

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // get the latest tbl
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();

    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let snapshot = reader.read(snapshot_loc.as_str(), None, 1).await?;

    // nothing is pruned
    let e1 = Extras {
        filters: vec![col("a", u64::to_data_type()).gt(&lit(30u64))?],
        ..Default::default()
    };

    // some blocks pruned
    let mut e2 = Extras::default();
    let max_val_of_b = 6u64;
    e2.filters = vec![
        col("a", u64::to_data_type())
            .gt(&lit(0u64))?
            .and(&col("b", u64::to_data_type()).gt(&lit(max_val_of_b))?)?,
    ];
    let b2 = num_blocks - max_val_of_b as usize - 1;

    // Sort asc Limit
    let e3 = Extras {
        order_by: vec![(col("b", u64::to_data_type()), true, false)],
        limit: Some(3),
        ..Default::default()
    };

    // Sort desc Limit
    let e4 = Extras {
        order_by: vec![(col("b", u64::to_data_type()), false, false)],
        limit: Some(4),
        ..Default::default()
    };

    let extras = vec![
        (None, num_blocks, num_blocks * row_per_block),
        (Some(e1), 0, 0),
        (Some(e2), b2, b2 * row_per_block),
        (Some(e3), 3, 3 * row_per_block),
        (Some(e4), 4, 4 * row_per_block),
    ];

    for (extra, expected_blocks, expected_rows) in extras {
        let blocks = apply_block_pruning(
            snapshot.clone(),
            table.get_table_info().schema(),
            &extra,
            ctx.clone(),
            fuse_table.get_operator(),
        )
        .await?;

        let rows = blocks.iter().map(|b| b.row_count as usize).sum::<usize>();
        assert_eq!(expected_rows, rows);
        assert_eq!(expected_blocks, blocks.len());
    }

    Ok(())
}

#[tokio::test]
async fn test_block_pruner_monotonic() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let test_tbl_name = "test_index_helper";
    let test_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);

    let row_per_block = 3u32;
    let num_blocks_opt = row_per_block.to_string();

    // create test table
    let create_table_plan = CreateTablePlanV2 {
        catalog: "default".to_owned(),
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        schema: test_schema.clone(),
        engine: Engine::Fuse,
        storage_params: None,
        options: [
            (FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
            // for the convenience of testing, let one seegment contains one block
            (FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_default_exprs: vec![],
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
    };

    let catalog = ctx.get_catalog("default")?;
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // get table
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let blocks = vec![
        DataBlock::create(test_schema.clone(), vec![
            Series::from_data(vec![1u64, 2, 3]),
            Series::from_data(vec![11u64, 12, 13]),
        ]),
        DataBlock::create(test_schema.clone(), vec![
            Series::from_data(vec![4u64, 5, 6]),
            Series::from_data(vec![21u64, 22, 23]),
        ]),
        DataBlock::create(test_schema, vec![
            Series::from_data(vec![7u64, 8, 9]),
            Series::from_data(vec![31u64, 32, 33]),
        ]),
    ];

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // get the latest tbl
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let snapshot = reader.read(snapshot_loc.as_str(), None, 1).await?;

    // a + b > 20; some blocks pruned
    let mut extra = Extras::default();
    let pred = add(col("a", u64::to_data_type()), col("b", u64::to_data_type())).gt(&lit(20u64))?;
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        snapshot.clone(),
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
        fuse_table.get_operator(),
    )
    .await?;

    assert_eq!(2, blocks.len());

    // b - a < 20; nothing will be pruned.
    let mut extra = Extras::default();
    let pred = sub(col("b", u64::to_data_type()), col("a", u64::to_data_type())).lt(&lit(20u64))?;
    extra.filters = vec![pred];

    let blocks = apply_block_pruning(
        snapshot.clone(),
        table.get_table_info().schema(),
        &Some(extra),
        ctx.clone(),
        fuse_table.get_operator(),
    )
    .await?;

    assert_eq!(3, blocks.len());

    Ok(())
}
