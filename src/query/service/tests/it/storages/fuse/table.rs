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

use std::default::Default;

use common_ast::ast::Engine;
use common_base::base::tokio;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::plans::AlterTableClusterKeyPlan;
use common_sql::plans::CreateTablePlanV2;
use common_sql::plans::DropTableClusterKeyPlan;
use common_storages_table_meta::table::OPT_KEY_DATABASE_ID;
use common_storages_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_query::interpreters::AlterTableClusterKeyInterpreter;
use databend_query::interpreters::CreateTableInterpreterV2;
use databend_query::interpreters::DropTableClusterKeyInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::storages::fuse::FuseTable;
use databend_query::stream::ReadDataBlockStream;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    fixture.create_default_table().await?;

    let mut table = fixture.latest_default_table().await?;

    // basic append and read
    {
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.seq;
        table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.seq);

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        ctx.try_set_partitions(parts.clone())?;
        let stream = table
            .read_data_block_stream(ctx.clone(), &DataSourcePlan {
                catalog: "default".to_owned(),
                source_info: DataSourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts,
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // recall our test setting:
        //   - num_blocks = 2;
        //   - rows_per_block = 2;
        //   - value_start_from = 1
        // thus
        let expected = vec![
            "+----+--------+", //
            "| id | t      |", //
            "+----+--------+", //
            "| 1  | (2, 3) |", //
            "| 1  | (2, 3) |", //
            "| 2  | (4, 6) |", //
            "| 2  | (4, 6) |", //
            "+----+--------+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
    }

    // test commit with overwrite

    {
        // insert overwrite 5 blocks
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 2;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, true, true)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.seq;
        let table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.seq);

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        // inject partitions to current ctx
        ctx.try_set_partitions(parts.clone())?;

        let stream = table
            .read_data_block_stream(ctx.clone(), &DataSourcePlan {
                catalog: "default".to_owned(),
                source_info: DataSourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts,
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // two block, two rows for each block, value starts with 2
        let expected = vec![
            "+----+--------+", //
            "| id | t      |", //
            "+----+--------+", //
            "| 2  | (4, 6) |", //
            "| 2  | (4, 6) |", //
            "| 3  | (6, 9) |", //
            "| 3  | (6, 9) |", //
            "+----+--------+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
    }

    Ok(())
}

// TODO move this to test/it/storages/fuse/operations
#[tokio::test]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.seq;
    let r = table.truncate(ctx.clone(), false).await;
    let table = fixture.latest_default_table().await?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().ident.seq);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let num_blocks = 10;
    let rows_per_block = 3;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let source_plan = table.read_plan(ctx.clone(), None).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);

    // ensure data ingested
    let (stats, _) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    assert_eq!(stats.read_rows, (num_blocks * rows_per_block));

    // truncate
    let purge = false;
    let r = table.truncate(ctx.clone(), purge).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}

// TODO move this to test/it/storages/fuse/operations
#[tokio::test]
async fn test_fuse_table_optimize() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_normal_table().await?;

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // there will be 5 blocks
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    assert_eq!(parts.len(), n);

    // do compact
    let query = format!("optimize table {}.{} compact", db_name, tbl_name);

    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(&query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;

    // `PipelineBuilder` will parallelize the table reading according to value of setting `max_threads`,
    // and `Table::read` will also try to de-queue read jobs preemptively. thus, the number of blocks
    // that `Table::append` takes are not deterministic (`append` is also executed in parallel in this case),
    // therefore, the final number of blocks varies.
    // To avoid flaky test, the value of setting `max_threads` is set to be 1, so that pipeline_builder will
    // only arrange one worker for the `ReadDataSourcePlan`.
    ctx.get_settings().set_max_threads(1)?;
    let data_stream = interpreter.execute(ctx.clone()).await?;
    let _ = data_stream.try_collect::<Vec<_>>();

    // verify compaction
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    // blocks are so tiny, they should be compacted into one
    assert_eq!(parts.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_fuse_alter_table_cluster_key() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let create_table_plan = CreateTablePlanV2 {
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        schema: TestFixture::default_schema(),
        engine: Engine::Fuse,
        storage_params: None,
        options: [
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_default_exprs: vec![],
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
    };

    // create test table
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // add cluster key
    let alter_table_cluster_key_plan = AlterTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        cluster_keys: vec!["id".to_string()],
    };
    let interpreter =
        AlterTableClusterKeyInterpreter::try_create(ctx.clone(), alter_table_cluster_key_plan)?;
    interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.cluster_keys, vec!["(id)".to_string()]);
    assert_eq!(table_info.meta.default_cluster_key_id, Some(0));

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let snapshot = reader.read(snapshot_loc.as_str(), None, 1).await?;
    let expected = Some((0, "(id)".to_string()));
    assert_eq!(snapshot.cluster_key_meta, expected);

    // drop cluster key
    let drop_table_cluster_key_plan = DropTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
    };
    let interpreter =
        DropTableClusterKeyInterpreter::try_create(ctx.clone(), drop_table_cluster_key_plan)?;
    interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.default_cluster_key, None);
    assert_eq!(table_info.meta.default_cluster_key_id, None);

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let snapshot = reader.read(snapshot_loc.as_str(), None, 1).await?;
    let expected = None;
    assert_eq!(snapshot.cluster_key_meta, expected);

    Ok(())
}

#[test]
fn test_parse_storage_prefix() -> Result<()> {
    let mut tbl_info = TableInfo::default();
    let db_id = 2;
    let tbl_id = 1;
    tbl_info.ident.table_id = tbl_id;
    tbl_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
    let prefix = FuseTable::parse_storage_prefix(&tbl_info)?;
    assert_eq!(format!("{}/{}", db_id, tbl_id), prefix);
    Ok(())
}
