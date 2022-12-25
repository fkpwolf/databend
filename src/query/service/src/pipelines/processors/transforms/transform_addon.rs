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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_exception::Result;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::CompoundChunkOperator;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;
use crate::sessions::QueryContext;
use crate::sql::evaluator::Evaluator;

pub struct TransformAddOn {
    default_nonexpr_fields: Vec<DataField>,

    expression_transform: CompoundChunkOperator,
    output_schema: DataSchemaRef,
}

impl TransformAddOn
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let mut default_exprs = Vec::new();
        let mut default_nonexpr_fields = Vec::new();

        for f in output_schema.fields() {
            if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    default_exprs.push(ChunkOperator::Map {
                        name: f.name().to_string(),
                        eval: Evaluator::eval_physical_scalar(&serde_json::from_str(
                            default_expr,
                        )?)?,
                    });
                } else {
                    default_nonexpr_fields.push(f.clone());
                }
            }
        }

        let func_ctx = ctx.try_get_function_context()?;
        let expression_transform = CompoundChunkOperator {
            ctx: func_ctx,
            operators: default_exprs,
        };

        Ok(Transformer::create(input, output, Self {
            default_nonexpr_fields,
            expression_transform,
            output_schema,
        }))
    }
}

impl Transform for TransformAddOn {
    const NAME: &'static str = "AddOnTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        block = self.expression_transform.transform(block)?;

        for f in &self.default_nonexpr_fields {
            let default_value = f.data_type().default_value();
            let column = f
                .data_type()
                .create_constant_column(&default_value, num_rows)?;

            block = block.add_column(column, f.clone())?;
        }
        block.resort(self.output_schema.clone())
    }
}
