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

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_datavalues::StructColumn;
use common_datavalues::StructType;
use common_datavalues::ToDataType;
use common_exception::Result;
use databend_query::storages::fuse::statistics::gen_columns_statistics;
use databend_query::storages::fuse::statistics::traverse;

fn gen_sample_block() -> (DataBlock, Vec<ColumnRef>) {
    //   sample message
    //
    //   struct {
    //      a: struct {
    //          b: struct {
    //             c: i64,
    //             d: f64,
    //          },
    //          e: f64
    //      }
    //      f : i64,
    //      g: f64,
    //   }

    let col_b_type = StructType::create(Some(vec!["c".to_owned(), "d".to_owned()]), vec![
        i64::to_data_type(),
        f64::to_data_type(),
    ]);

    let col_a_type = StructType::create(Some(vec!["b".to_owned(), "e".to_owned()]), vec![
        DataTypeImpl::Struct(col_b_type.clone()),
        f64::to_data_type(),
    ]);

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataTypeImpl::Struct(col_a_type.clone())),
        DataField::new("f", i64::to_data_type()),
        DataField::new("g", f64::to_data_type()),
    ]);

    // prepare leaves
    let col_c = Series::from_data(vec![1i64, 2, 3]);
    let col_d = Series::from_data(vec![1.0f64, 2., 3.]);
    let col_e = Series::from_data(vec![4.0f64, 5., 6.]);
    let col_f = Series::from_data(vec![7i64, 8, 9]);
    let col_g = Series::from_data(vec![10.0f64, 11., 12.]);

    // inner/root nodes
    let col_b: ColumnRef = Arc::new(StructColumn::from_data(
        vec![col_c.clone(), col_d.clone()],
        DataTypeImpl::Struct(col_b_type),
    ));
    let col_a: ColumnRef = Arc::new(StructColumn::from_data(
        vec![col_b.clone(), col_e.clone()],
        DataTypeImpl::Struct(col_a_type),
    ));

    (
        DataBlock::create(schema, vec![col_a.clone(), col_f.clone(), col_g.clone()]),
        vec![col_c, col_d, col_e, col_f, col_g],
    )
}

#[test]
fn test_column_traverse() -> Result<()> {
    let (sample_block, sample_cols) = gen_sample_block();
    let cols = traverse::traverse_columns_dfs(sample_block.columns())?;

    assert_eq!(5, cols.len());
    (0..5).for_each(|i| assert_eq!(cols[i].1, sample_cols[i], "checking col {}", i));

    Ok(())
}

#[test]
fn test_column_statistic() -> Result<()> {
    let (sample_block, sample_cols) = gen_sample_block();
    let col_stats = gen_columns_statistics(&sample_block, None)?;

    assert_eq!(5, col_stats.len());

    (0..5).for_each(|i| {
        let stats = col_stats.get(&(i as u32)).unwrap();
        let column = &sample_cols[i];
        let values: Vec<DataValue> = (0..column.len()).map(|i| column.get(i)).collect();
        assert_eq!(
            &stats.min,
            values.iter().min().unwrap(),
            "checking min of col {}",
            i
        );
        assert_eq!(
            &stats.max,
            values.iter().max().unwrap(),
            "checking max of col {}",
            i
        );
    });

    Ok(())
}
