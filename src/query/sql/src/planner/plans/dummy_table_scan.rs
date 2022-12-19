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

use common_exception::Result;

use super::LogicalOperator;
use super::Operator;
use super::PhysicalOperator;
use crate::optimizer::ColumnSet;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelationalProperty;
use crate::optimizer::Statistics;
use crate::PlannerContext;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DummyTableScan;

impl Operator for DummyTableScan {
    fn rel_op(&self) -> super::RelOp {
        super::RelOp::DummyTableScan
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

impl LogicalOperator for DummyTableScan {
    fn derive_relational_prop<'a>(
        &self,
        _rel_expr: &crate::optimizer::RelExpr<'a>,
    ) -> Result<RelationalProperty> {
        Ok(RelationalProperty {
            output_columns: ColumnSet::new(),
            outer_columns: ColumnSet::new(),
            used_columns: ColumnSet::new(),
            cardinality: 1.0,
            statistics: Statistics {
                precise_cardinality: Some(1),
                column_stats: Default::default(),
                is_accurate: false,
            },
        })
    }

    fn used_columns<'a>(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}

impl PhysicalOperator for DummyTableScan {
    fn derive_physical_prop<'a>(
        &self,
        _rel_expr: &crate::optimizer::RelExpr<'a>,
    ) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: crate::optimizer::Distribution::Serial,
        })
    }

    fn compute_required_prop_child<'a>(
        &self,
        _ctx: Arc<dyn PlannerContext>,
        _rel_expr: &crate::optimizer::RelExpr<'a>,
        _child_index: usize,
        required: &crate::optimizer::RequiredProperty,
    ) -> Result<crate::optimizer::RequiredProperty> {
        Ok(required.clone())
    }
}
