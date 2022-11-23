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

use common_exception::ErrorCode;
use common_exception::Result;

use super::RelationalProperty;
use crate::optimizer::rule::AppliedRules;
use crate::optimizer::rule::RuleID;
use crate::optimizer::TableSet;
use crate::plans::Operator;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Clone, Debug)]
pub struct SExpr {
    pub(crate) plan: RelOperator,
    pub(crate) children: Vec<SExpr>,

    pub(crate) original_group: Option<IndexType>,
    pub(crate) rel_prop: Option<Box<RelationalProperty>>,

    /// A bitmap to record applied rules on current SExpr, to prevent
    /// redundant transformations.
    pub(crate) applied_rules: AppliedRules,
}

impl SExpr {
    pub fn create(
        plan: RelOperator,
        children: Vec<SExpr>,
        original_group: Option<IndexType>,
        rel_prop: Option<Box<RelationalProperty>>,
    ) -> Self {
        SExpr {
            plan,
            children,
            original_group,
            rel_prop,

            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary(plan: RelOperator, child: SExpr) -> Self {
        Self::create(plan, vec![child], None, None)
    }

    pub fn create_binary(plan: RelOperator, left_child: SExpr, right_child: SExpr) -> Self {
        Self::create(plan, vec![left_child, right_child], None, None)
    }

    pub fn create_leaf(plan: RelOperator) -> Self {
        Self::create(plan, vec![], None, None)
    }

    pub fn create_pattern_leaf() -> Self {
        Self::create(
            PatternPlan {
                plan_type: RelOp::Pattern,
            }
            .into(),
            vec![],
            None,
            None,
        )
    }

    pub fn plan(&self) -> &RelOperator {
        &self.plan
    }

    pub fn children(&self) -> &[SExpr] {
        &self.children
    }

    pub fn child(&self, n: usize) -> Result<&SExpr> {
        self.children
            .get(n)
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid children index: {}", n)))
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn is_pattern(&self) -> bool {
        matches!(self.plan.rel_op(), RelOp::Pattern)
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.original_group
    }

    pub fn match_pattern(&self, pattern: &SExpr) -> bool {
        if pattern.plan.rel_op() != RelOp::Pattern {
            // Pattern is plan
            if self.plan.rel_op() != pattern.plan.rel_op() {
                return false;
            }

            if self.arity() != pattern.arity() {
                // Check if current expression has same arity with current pattern
                return false;
            }

            for (e, p) in self.children.iter().zip(pattern.children.iter()) {
                // Check children
                if !e.match_pattern(p) {
                    return false;
                }
            }
        };

        true
    }

    pub fn replace_children(&self, children: Vec<SExpr>) -> Self {
        Self {
            plan: self.plan.clone(),
            original_group: self.original_group,
            rel_prop: self.rel_prop.clone(),
            applied_rules: self.applied_rules.clone(),
            children,
        }
    }

    /// Record the applied rule id in current SExpr
    pub(crate) fn apply_rule(&mut self, rule_id: &RuleID) {
        self.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current SExpr
    pub(crate) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.applied_rules.get(rule_id)
    }

    pub(crate) fn used_tables(&self) -> Result<TableSet> {
        match &self.plan {
            RelOperator::LogicalGet(operator) => {
                let mut table_set = TableSet::new();
                table_set.insert(operator.table_index);
                Ok(table_set)
            }
            RelOperator::PhysicalScan(operator) => {
                let mut table_set = TableSet::new();
                table_set.insert(operator.table_index);
                Ok(table_set)
            }
            RelOperator::LogicalJoin(_)
            | RelOperator::PhysicalHashJoin(_)
            | RelOperator::UnionAll(_) => {
                let mut table_set = TableSet::new();
                table_set.extend(self.child(0)?.used_tables()?);
                table_set.extend(self.child(1)?.used_tables()?);
                Ok(table_set)
            }
            RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::Exchange(_) => {
                let mut table_set = TableSet::new();
                table_set.extend(self.child(0)?.used_tables()?);
                Ok(table_set)
            }
            RelOperator::DummyTableScan(_) | RelOperator::Pattern(_) => Ok(TableSet::new()),
        }
    }
}
