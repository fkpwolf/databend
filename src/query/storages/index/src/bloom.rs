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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planner::Expression;

use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::filters::Xor8Builder;
use crate::filters::Xor8Filter;
use crate::SupportedType;

/// BlockFilter represents multiple per-column filters(bloom filter or xor filter etc) for data block.
///
/// By default we create a filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a filter, we skip the creation.
/// That is to say, it is legal to have a BlockFilter with zero columns.
///
/// For example, for the source data block as follows:
/// ```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create table of filters as follows:
/// ```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BlockFilter {
    /// The schema of the source table/block, which the filter work for.
    pub source_schema: DataSchemaRef,

    /// The schema of the filter block.
    ///
    /// It is a sub set of `source_schema`.
    pub filter_schema: DataSchemaRef,

    /// Data block of filters;
    pub filter_block: DataBlock,
}

/// FilterExprEvalResult represents the evaluation result of an expression by a filter.
///
/// For example, expression of 'age = 12' should return false is the filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Maybe'.
///
/// If the column is not applicable for a filter, like TypeID::struct, NotApplicable is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterEvalResult {
    False,
    Maybe,
    NotApplicable,
}

impl BlockFilter {
    /// For every applicable column, we will create a filter.
    /// The filter will be stored with field name 'Bloom(column_name)'
    pub fn build_filter_column_name(column_name: &str) -> String {
        format!("Bloom({})", column_name)
    }
    pub fn build_filter_schema(data_schema: &DataSchema) -> DataSchema {
        let mut filter_fields = vec![];
        let fields = data_schema.fields();
        for field in fields.iter() {
            if Xor8Filter::is_supported_type(field.data_type()) {
                // create field for applicable ones

                let column_name = Self::build_filter_column_name(field.name());
                let filter_field = DataField::new(&column_name, Vu8::to_data_type());

                filter_fields.push(filter_field);
            }
        }

        DataSchema::new(filter_fields)
    }

    /// Load a filter directly from the source table's schema and the corresponding filter parquet file.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn from_filter_block(
        source_table_schema: DataSchemaRef,
        filter_block: DataBlock,
    ) -> Result<Self> {
        Ok(Self {
            source_schema: source_table_schema,
            filter_schema: filter_block.schema().clone(),
            filter_block,
        })
    }

    /// Create a filter block from source data.
    ///
    /// All input blocks should belong to a Parquet file, e.g. the block array represents the parquet file in memory.
    pub fn try_create(blocks: &[&DataBlock]) -> Result<Self> {
        if blocks.is_empty() {
            return Err(ErrorCode::BadArguments("data blocks is empty"));
        }

        let source_schema = blocks[0].schema().clone();
        let mut filter_columns = vec![];

        let fields = source_schema.fields();
        for (i, field) in fields.iter().enumerate() {
            if Xor8Filter::is_supported_type(field.data_type()) {
                // create filter per column
                let mut filter_builder = Xor8Builder::create();

                // ingest the same column data from all blocks
                for block in blocks.iter() {
                    let col = block.column(i);
                    filter_builder.add_keys(&col.to_values());
                }

                let filter = filter_builder.build()?;

                // create filter column

                let serialized_bytes = filter.to_bytes()?;
                let filter_value = DataValue::String(serialized_bytes);

                let filter_column: ColumnRef =
                    filter_value.as_const_column(&StringType::new_impl(), 1)?;
                filter_columns.push(filter_column);
            }
        }

        let filter_schema = Arc::new(Self::build_filter_schema(source_schema.as_ref()));
        let filter_block = DataBlock::create(filter_schema.clone(), filter_columns);
        Ok(Self {
            source_schema,
            filter_schema,
            filter_block,
        })
    }

    pub fn find(
        &self,
        column_name: &str,
        target: DataValue,
        typ: &DataTypeImpl,
    ) -> Result<FilterEvalResult> {
        let filter_column = Self::build_filter_column_name(column_name);
        if !self.filter_block.schema().has_field(&filter_column)
            || !Xor8Filter::is_supported_type(typ)
            || target.is_null()
        {
            // The column doesn't a filter
            return Ok(FilterEvalResult::NotApplicable);
        }

        let filter_bytes = self.filter_block.first(&filter_column)?.as_string()?;
        let (filter, _size) = Xor8Filter::from_bytes(&filter_bytes)?;
        if filter.contains(&target) {
            Ok(FilterEvalResult::Maybe)
        } else {
            Ok(FilterEvalResult::False)
        }
    }

    /// Returns false when the expression must be false, otherwise true.
    /// The 'true' doesn't really mean the expression is true, but 'maybe true'.
    /// That is to say, you still need the load all data and run the execution.
    pub fn maybe_true(&self, expr: &Expression) -> Result<bool> {
        Ok(self.eval(expr)? != FilterEvalResult::False)
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in the filter.
    ///
    /// Otherwise return either Maybe or NotApplicable.
    #[tracing::instrument(level = "debug", name = "block_filter_index_eval", skip_all)]
    pub fn eval(&self, expr: &Expression) -> Result<FilterEvalResult> {
        // TODO: support multiple columns and other ops like 'in' ...
        match expr {
            Expression::Function { name, args, .. } if args.len() == 2 => {
                match name.to_lowercase().as_str() {
                    "=" => self.eval_equivalent_expression(&args[0], &args[1]),
                    "and" => self.eval_logical_and(&args[0], &args[1]),
                    "or" => self.eval_logical_or(&args[0], &args[1]),
                    _ => Ok(FilterEvalResult::NotApplicable),
                }
            }
            _ => Ok(FilterEvalResult::NotApplicable),
        }
    }

    // Evaluate the equivalent expression like "name='Alice'"
    fn eval_equivalent_expression(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Result<FilterEvalResult> {
        let schema: &DataSchemaRef = &self.source_schema;

        // For now only support single column like "name = 'Alice'"
        match (left, right) {
            // match the expression of 'column_name = literal constant'
            (Expression::IndexedVariable { name, .. }, Expression::Constant { value, .. })
            | (Expression::Constant { value, .. }, Expression::IndexedVariable { name, .. }) => {
                // find the corresponding column from source table
                let data_field = schema.field_with_name(name)?;
                let data_type = data_field.data_type();

                // check if cast needed
                let value = if &value.data_type() != data_type {
                    let col = value.as_const_column(data_type, 1)?;
                    col.get_checked(0)?
                } else {
                    value.clone()
                };
                self.find(name, value, data_type)
            }
            _ => Ok(FilterEvalResult::NotApplicable),
        }
    }

    // Evaluate the logical and expression
    fn eval_logical_and(&self, left: &Expression, right: &Expression) -> Result<FilterEvalResult> {
        let left_result = self.eval(left)?;
        if left_result == FilterEvalResult::False {
            return Ok(FilterEvalResult::False);
        }

        let right_result = self.eval(right)?;
        if right_result == FilterEvalResult::False {
            return Ok(FilterEvalResult::False);
        }

        if left_result == FilterEvalResult::NotApplicable
            || right_result == FilterEvalResult::NotApplicable
        {
            Ok(FilterEvalResult::NotApplicable)
        } else {
            Ok(FilterEvalResult::Maybe)
        }
    }

    // Evaluate the logical or expression
    fn eval_logical_or(&self, left: &Expression, right: &Expression) -> Result<FilterEvalResult> {
        let left_result = self.eval(left)?;
        let right_result = self.eval(right)?;
        match (&left_result, &right_result) {
            (&FilterEvalResult::False, &FilterEvalResult::False) => Ok(FilterEvalResult::False),
            (&FilterEvalResult::False, _) => Ok(right_result),
            (_, &FilterEvalResult::False) => Ok(left_result),
            (&FilterEvalResult::Maybe, &FilterEvalResult::Maybe) => Ok(FilterEvalResult::Maybe),
            _ => Ok(FilterEvalResult::NotApplicable),
        }
    }
}
