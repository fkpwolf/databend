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

use std::ops::Not;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::Series;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_planner::plans::Projection;
use common_planner::Expression;
use common_sql::evaluator::Evaluator;

use crate::operations::mutation::deletion_mutator::Deletion;
use crate::FuseTable;

pub async fn delete_from_block(
    table: &FuseTable,
    block_meta: &BlockMeta,
    ctx: &Arc<dyn TableContext>,
    filter_column_proj: Projection,
    filter_expr: &Expression,
) -> Result<Deletion> {
    let mut filtering_whole_block = false;

    // extract the columns that are going to be filtered on
    let proj = {
        if filter_column_proj.is_empty() {
            // here the situation: filter_expr is not null, but filter_column_ids in not empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // tobe refined:
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            filtering_whole_block = true;
            let all_col_ids = all_the_columns_ids(table);
            Projection::Columns(all_col_ids)
        } else {
            filter_column_proj
        }
    };

    // read the cols that we are going to filtering on
    let reader = table.create_block_reader(proj)?;
    let data_block = reader.read_with_block_meta(block_meta).await?;

    let eval_node = Evaluator::eval_expression(filter_expr, data_block.schema().as_ref())?;
    let filter_result = eval_node
        .eval(&ctx.try_get_function_context()?, &data_block)?
        .vector;
    let predicates = DataBlock::cast_to_nonull_boolean(&filter_result)?;

    // shortcut, if predicates is const boolean (or can be cast to boolean)
    if let Some(const_bool) = DataBlock::try_as_const_bool(&predicates)? {
        return if const_bool {
            // all the rows should be removed
            Ok(Deletion::Remains(DataBlock::empty_with_schema(
                data_block.schema().clone(),
            )))
        } else {
            // none of the rows should be removed
            Ok(Deletion::NothingDeleted)
        };
    }

    // reverse the filter
    let boolean_col: &BooleanColumn = Series::check_get(&predicates)?;
    let values = boolean_col.values();
    let filter = BooleanColumn::from_arrow_data(values.not());

    // read the whole block if necessary
    let whole_block = if filtering_whole_block {
        data_block
    } else {
        let all_col_ids = all_the_columns_ids(table);
        let whole_table_proj = Projection::Columns(all_col_ids);
        let whole_block_reader = table.create_block_reader(whole_table_proj)?;
        whole_block_reader.read_with_block_meta(block_meta).await?
    };

    // filter out rows
    let data_block = DataBlock::filter_block_with_bool_column(whole_block, &filter)?;

    let res = if data_block.num_rows() == block_meta.row_count as usize {
        // false positive, nothing removed indeed
        Deletion::NothingDeleted
    } else {
        Deletion::Remains(data_block)
    };
    Ok(res)
}

pub fn all_the_columns_ids(table: &FuseTable) -> Vec<usize> {
    (0..table.table_info.schema().fields().len())
        .into_iter()
        .collect::<Vec<usize>>()
}
