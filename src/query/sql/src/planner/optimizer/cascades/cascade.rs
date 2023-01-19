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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::cascades::explore_rules::get_explore_rule_set;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::OptimizeGroupTask;
use crate::optimizer::cascades::tasks::Task;
use crate::optimizer::cost::CostContext;
use crate::optimizer::cost::CostModel;
use crate::optimizer::cost::DefaultCostModel;
use crate::optimizer::format::display_memo;
use crate::optimizer::memo::Memo;
use crate::optimizer::rule::RuleSet;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::IndexType;

/// A cascades-style search engine to enumerate possible alternations of a relational expression and
/// find the optimal one.
pub struct CascadesOptimizer {
    pub memo: Memo,
    pub explore_rules: RuleSet,

    pub cost_model: Box<dyn CostModel>,

    /// group index -> best cost context
    pub best_cost_map: HashMap<IndexType, CostContext>,
    _ctx: Arc<dyn TableContext>,
}

impl CascadesOptimizer {
    pub fn create(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let explore_rules = if ctx.get_settings().get_enable_cbo()? {
            let enable_bushy_join = ctx.get_settings().get_enable_bushy_join()? != 0;
            get_explore_rule_set(enable_bushy_join)
        } else {
            RuleSet::create_with_ids(vec![]).unwrap()
        };
        Ok(CascadesOptimizer {
            memo: Memo::create(),
            explore_rules,
            cost_model: Box::new(DefaultCostModel),
            best_cost_map: HashMap::new(),
            _ctx: ctx,
        })
    }

    fn init(&mut self, expression: SExpr) -> Result<()> {
        self.memo.init(expression)?;

        Ok(())
    }

    pub fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.init(s_expr)?;

        let root_index = self
            .memo
            .root()
            .ok_or_else(|| ErrorCode::Internal("Root group cannot be None after initialization"))?
            .group_index;

        let root_task = OptimizeGroupTask::new(root_index);
        let mut scheduler = Scheduler::new();
        scheduler.add_task(Task::OptimizeGroup(root_task));
        scheduler.run(self)?;

        tracing::debug!("Memo:\n{}", display_memo(&self.memo, &self.best_cost_map)?);

        self.find_optimal_plan(root_index)
    }

    pub fn insert_from_transform_state(
        &mut self,
        group_index: IndexType,
        state: TransformResult,
    ) -> Result<()> {
        for result in state.results() {
            self.insert_expression(group_index, result)?;
        }

        Ok(())
    }

    fn insert_expression(&mut self, group_index: IndexType, expression: &SExpr) -> Result<()> {
        self.memo.insert(Some(group_index), expression.clone())?;

        Ok(())
    }

    fn find_optimal_plan(&self, group_index: IndexType) -> Result<SExpr> {
        let group = self.memo.group(group_index)?;
        let cost_context = self.best_cost_map.get(&group_index).ok_or_else(|| {
            ErrorCode::Internal(format!("Cannot find CostContext of group: {group_index}"))
        })?;

        let m_expr = group.m_exprs.get(cost_context.expr_index).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot find best expression of group: {group_index}"
            ))
        })?;

        let children = m_expr
            .children
            .iter()
            .map(|index| self.find_optimal_plan(*index))
            .collect::<Result<Vec<_>>>()?;

        let result = SExpr::create(m_expr.plan.clone(), children, None, None);

        Ok(result)
    }
}
