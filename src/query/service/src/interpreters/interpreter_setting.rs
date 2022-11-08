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

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::SettingPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SettingInterpreter {
    ctx: Arc<QueryContext>,
    set: SettingPlan,
}

impl SettingInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, set: SettingPlan) -> Result<Self> {
        Ok(SettingInterpreter { ctx, set })
    }
}

#[async_trait::async_trait]
impl Interpreter for SettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.set.clone();
        for var in plan.vars {
            let ok = match var.variable.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => false,
                "timezone" => {
                    // check if the timezone is valid
                    let tz = var.value.trim_matches(|c| c == '\'' || c == '\"');
                    let _ = tz.parse::<Tz>().map_err(|_| {
                        ErrorCode::InvalidTimezone(format!("Invalid Timezone: {}", var.value))
                    })?;
                    self.ctx.get_settings().set_settings(
                        var.variable.clone(),
                        tz.to_string(),
                        var.is_global,
                    )?;
                    true
                }
                _ => {
                    self.ctx.get_settings().set_settings(
                        var.variable.clone(),
                        var.value.clone(),
                        var.is_global,
                    )?;
                    true
                }
            };
            if ok {
                self.ctx.set_affect(QueryAffect::ChangeSetting {
                    key: var.variable.clone(),
                    value: var.value.clone(),
                    is_global: var.is_global,
                })
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
