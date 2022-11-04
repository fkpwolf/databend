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

use std::iter::once;
use std::sync::Arc;

use common_arrow::arrow::array::ord as arrow_ord;
use common_arrow::arrow::array::ord::DynComparator;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute::merge_sort::*;
use common_arrow::arrow::compute::sort as arrow_sort;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::error::Error as ArrowError;
use common_arrow::arrow::error::Result as ArrowResult;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

pub type Aborting = Arc<Box<dyn Fn() -> bool + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool,
}

impl DataBlock {
    pub fn sort_block(
        block: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let order_columns = sort_columns_descriptions
            .iter()
            .map(|f| {
                let c = block.try_column_by_name(&f.column_name)?;
                Ok(c.as_arrow_array(c.data_type()))
            })
            .collect::<Result<Vec<_>>>()?;

        let order_arrays = sort_columns_descriptions
            .iter()
            .zip(order_columns.iter())
            .map(|(f, array)| {
                Ok(arrow_sort::SortColumn {
                    values: array.as_ref(),
                    options: Some(arrow_sort::SortOptions {
                        descending: !f.asc,
                        nulls_first: f.nulls_first,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices: PrimitiveArray<u32> =
            arrow_sort::lexsort_to_indices_impl(&order_arrays, limit, &build_compare)?;
        DataBlock::block_take_by_indices(block, indices.values())
    }

    pub fn merge_sort_block(
        lhs: &DataBlock,
        rhs: &DataBlock,
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        if lhs.num_rows() == 0 {
            return Ok(rhs.clone());
        }

        if rhs.num_rows() == 0 {
            return Ok(lhs.clone());
        }

        let sort_arrays = sort_columns_descriptions
            .iter()
            .map(|f| {
                let left = lhs.try_column_by_name(&f.column_name)?.clone();
                let left = left.as_arrow_array(left.data_type());

                let right = rhs.try_column_by_name(&f.column_name)?.clone();
                let right = right.as_arrow_array(right.data_type());

                Ok(vec![left, right])
            })
            .collect::<Result<Vec<_>>>()?;

        let sort_dyn_arrays = sort_arrays
            .iter()
            .map(|f| vec![f[0].as_ref(), f[1].as_ref()])
            .collect::<Vec<_>>();

        let sort_options = sort_columns_descriptions
            .iter()
            .map(|f| arrow_sort::SortOptions {
                descending: !f.asc,
                nulls_first: f.nulls_first,
            })
            .collect::<Vec<_>>();

        let sort_options_with_array = sort_dyn_arrays
            .iter()
            .zip(sort_options.iter())
            .map(|(s, opt)| {
                let paris: (&[&dyn Array], &SortOptions) = (s, opt);
                paris
            })
            .collect::<Vec<_>>();

        let comparator = build_comparator_impl(&sort_options_with_array, &build_compare)?;
        let lhs_indices = (0, 0, lhs.num_rows());
        let rhs_indices = (1, 0, rhs.num_rows());
        let slices = merge_sort_slices(once(&lhs_indices), once(&rhs_indices), &comparator);
        let slices = slices.to_vec(limit);

        let fields = lhs.schema().fields();
        let columns = fields
            .iter()
            .map(|f| {
                let left = lhs.try_column_by_name(f.name())?;
                let right = rhs.try_column_by_name(f.name())?;
                Self::take_column_by_slices_limit(
                    f.data_type(),
                    &[left.clone(), right.clone()],
                    &slices,
                    limit,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(lhs.schema().clone(), columns))
    }

    pub fn merge_sort_blocks(
        blocks: &[DataBlock],
        sort_columns_descriptions: &[SortColumnDescription],
        limit: Option<usize>,
        aborting: Aborting,
    ) -> Result<DataBlock> {
        match blocks.len() {
            0 => Result::Err(ErrorCode::EmptyData("Can't merge empty blocks")),
            1 => Ok(blocks[0].clone()),
            2 => {
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                DataBlock::merge_sort_block(
                    &blocks[0],
                    &blocks[1],
                    sort_columns_descriptions,
                    limit,
                )
            }
            _ => {
                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                let left = DataBlock::merge_sort_blocks(
                    &blocks[0..blocks.len() / 2],
                    sort_columns_descriptions,
                    limit,
                    aborting.clone(),
                )?;

                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                let right = DataBlock::merge_sort_blocks(
                    &blocks[blocks.len() / 2..blocks.len()],
                    sort_columns_descriptions,
                    limit,
                    aborting.clone(),
                )?;

                if aborting() {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                DataBlock::merge_sort_block(&left, &right, sort_columns_descriptions, limit)
            }
        }
    }
}

fn compare_variant(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = VariantColumn::from_arrow_array(left);
    let right = VariantColumn::from_arrow_array(right);
    Ok(Box::new(move |i, j| {
        left.get_data(i).cmp(right.get_data(j))
    }))
}

fn compare_array(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    let left = ArrayColumn::from_arrow_array(left);
    let right = ArrayColumn::from_arrow_array(right);

    Ok(Box::new(move |i, j| {
        left.get_data(i).cmp(&right.get_data(j))
    }))
}

fn build_compare(left: &dyn Array, right: &dyn Array) -> ArrowResult<DynComparator> {
    match left.data_type() {
        ArrowType::LargeList(_) => compare_array(left, right),
        ArrowType::Extension(name, _, _) => {
            if name == "Variant" || name == "VariantArray" || name == "VariantObject" {
                compare_variant(left, right)
            } else {
                Err(ArrowError::NotYetImplemented(format!(
                    "Sort not supported for data type {:?}",
                    left.data_type()
                )))
            }
        }
        _ => arrow_ord::build_compare(left, right),
    }
}
