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

use common_catalog::table::TableStatistics;
use common_exception::Result;
use itertools::Itertools;

use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::plans::LogicalOperator;
use crate::plans::Operator;
use crate::plans::PhysicalOperator;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::plans::SortItem;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prewhere {
    // columns needed to be output after prewhere scan
    pub output_columns: ColumnSet,
    // columns needed to conduct prewhere filter
    pub prewhere_columns: ColumnSet,
    // prewhere filter predicates
    pub predicates: Vec<Scalar>,
}

#[derive(Clone, Debug)]
pub struct LogicalGet {
    pub table_index: IndexType,
    pub columns: ColumnSet,
    pub push_down_predicates: Option<Vec<Scalar>>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<SortItem>>,
    pub prewhere: Option<Prewhere>,

    // statistics will be ignored in comparison and hashing
    pub statistics: Option<TableStatistics>,
}

impl PartialEq for LogicalGet {
    fn eq(&self, other: &Self) -> bool {
        self.table_index == other.table_index
            && self.columns == other.columns
            && self.push_down_predicates == other.push_down_predicates
    }
}

impl Eq for LogicalGet {}

impl std::hash::Hash for LogicalGet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_index.hash(state);
        for column in self.columns.iter().sorted() {
            column.hash(state);
        }
        self.push_down_predicates.hash(state);
    }
}

impl Operator for LogicalGet {
    fn rel_op(&self) -> RelOp {
        RelOp::LogicalGet
    }

    fn is_physical(&self) -> bool {
        false
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        None
    }
}

impl LogicalOperator for LogicalGet {
    fn derive_relational_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let mut used_columns = ColumnSet::new();
        if let Some(preds) = &self.push_down_predicates {
            for pred in preds.iter() {
                used_columns.extend(pred.used_columns());
            }
        }
        if let Some(prewhere) = &self.prewhere {
            used_columns.extend(prewhere.prewhere_columns.iter());
        }

        Ok(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns,
            cardinality: self
                .statistics
                .as_ref()
                .map_or(0.0, |stat| stat.num_rows.map_or(0.0, |num| num as f64)),
            precise_cardinality: self.statistics.as_ref().and_then(|stat| stat.num_rows),

            column_stats: Default::default(),
        })
    }

    fn used_columns<'a>(&self) -> Result<ColumnSet> {
        Ok(self.columns.clone())
    }
}
