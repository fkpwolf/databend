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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

pub struct RuleEliminateEvalScalar {
    id: RuleID,
    pattern: SExpr,
}

impl RuleEliminateEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateEvalScalar,
            // EvalScalar
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::EvalScalar,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleEliminateEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;

        // Eliminate empty EvalScalar
        if eval_scalar.items.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
            return Ok(());
        }

        // TODO(leiysky): Use another rule to do this.
        // Remove trivial column reference scalar.
        if eval_scalar.items.iter().any(|item| matches!(&item.scalar, ScalarExpr::BoundColumnRef(column) if column.column.index == item.index)) {
            let new_items = eval_scalar
                .items
                .into_iter()
                .filter(|item| !matches!(&item.scalar, ScalarExpr::BoundColumnRef(column) if column.column.index == item.index))
                .collect::<Vec<ScalarItem>>();
            if new_items.is_empty() {
                state.add_result(s_expr.child(0)?.clone());
            } else {
                state.add_result(SExpr::create_unary(
                    EvalScalar { items: new_items }.into(),
                    s_expr.child(0)?.clone(),
                ));
            }
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
