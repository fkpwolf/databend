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
use common_planner::plans::DescribeTablePlan;
use common_sql::executor::PhysicalScalar;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::Planner;
use crate::storages::view::view_table::QUERY;
use crate::storages::view::view_table::VIEW_ENGINE;

pub struct DescribeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeTablePlan,
}

impl DescribeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescribeTablePlan) -> Result<Self> {
        Ok(DescribeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeTableInterpreter {
    fn name(&self) -> &str {
        "DescribeTableInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.plan.catalog.as_str();
        let database = self.plan.database.as_str();
        let table = self.plan.table.as_str();
        let table = self.ctx.get_table(catalog, database, table).await?;
        let tbl_info = table.get_table_info();

        let schema = if tbl_info.engine() == VIEW_ENGINE {
            if let Some(query) = tbl_info.options().get(QUERY) {
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _, _) = planner.plan_sql(query).await?;
                plan.schema()
            } else {
                return Err(ErrorCode::Internal(
                    "Logical error, View Table must have a SelectQuery inside.",
                ));
            }
        } else {
            table.schema()
        };

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<String> = vec![];
        let mut default_exprs: Vec<String> = vec![];
        let mut extras: Vec<String> = vec![];

        for field in schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = remove_nullable(field.data_type());
            types.push(format_data_type_sql(&non_null_type));
            nulls.push(if field.is_nullable() {
                "YES".to_string()
            } else {
                "NO".to_string()
            });
            match field.default_expr() {
                Some(expr) => {
                    let expression: PhysicalScalar = serde_json::from_str(expr)?;
                    default_exprs.push(format!("{expression}"));
                }

                None => {
                    let value = field.data_type().default_value();
                    default_exprs.push(value.to_string());
                }
            }
            extras.push("".to_string());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::create(self.plan.schema(), vec![
            Series::from_data(names),
            Series::from_data(types),
            Series::from_data(nulls),
            Series::from_data(default_exprs),
            Series::from_data(extras),
        ])])
    }
}
