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
use common_exception::Result;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::Statistics;
use crate::plans::LogicalOperator;
use crate::plans::Operator;
use crate::plans::PhysicalOperator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum AggregateMode {
    Partial,
    Final,

    // TODO(leiysky): this mode is only used for preventing recursion of
    // RuleSplitAggregate, find a better way.
    Initial,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub mode: AggregateMode,
    // group by scalar expressions, such as: group by col1 asc, col2 desc;
    pub group_items: Vec<ScalarItem>,
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_functions: Vec<ScalarItem>,
    // True if the plan is generated from distinct, else the plan is a normal aggregate;
    pub from_distinct: bool,
}

impl Operator for Aggregate {
    fn rel_op(&self) -> RelOp {
        RelOp::Aggregate
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }
}

impl PhysicalOperator for Aggregate {
    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn compute_required_prop_child<'a>(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr<'a>,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(required);
        }

        match self.mode {
            AggregateMode::Partial => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Any;
                } else {
                    // Group aggregation, enforce `Hash` distribution
                    required.distribution =
                        Distribution::Hash(vec![self.group_items[0].scalar.clone()]);
                }
            }

            AggregateMode::Final => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Serial;
                } else {
                    // The distribution should have been derived by partial aggregation
                    required.distribution = Distribution::Any;
                }
            }

            AggregateMode::Initial => unreachable!(),
        }
        Ok(required)
    }
}

impl LogicalOperator for Aggregate {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            output_columns.insert(group_item.index);
        }
        for agg in self.aggregate_functions.iter() {
            output_columns.insert(agg.index);
        }

        // Derive outer columns
        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        // Derive cardinality. We can not estimate the cardinality of an aggregate with group by, until
        // we have information about distribution of group keys. So we pass through the cardinality.
        let cardinality = if self.group_items.is_empty() {
            // Scalar aggregation
            1.0
        } else {
            input_prop.cardinality
        };

        let precise_cardinality = if self.group_items.is_empty() {
            Some(1)
        } else {
            None
        };

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns);
        let column_stats = input_prop.statistics.column_stats;
        let is_accurate = input_prop.statistics.is_accurate;

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats,
                is_accurate,
            },
        })
    }

    fn used_columns<'a>(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            used_columns.insert(group_item.index);
            used_columns.extend(group_item.scalar.used_columns())
        }
        for agg in self.aggregate_functions.iter() {
            used_columns.insert(agg.index);
            used_columns.extend(agg.scalar.used_columns())
        }
        Ok(used_columns)
    }
}
