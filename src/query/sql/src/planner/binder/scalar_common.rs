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

use common_exception::Result;
use common_expression::types::DataType;

use crate::binder::scalar_visitor::Recursion;
use crate::binder::scalar_visitor::ScalarVisitor;
use crate::optimizer::RelationalProperty;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::Scalar;
use crate::plans::ScalarExpr;

// Visitor that find Expressions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    find_fn: &'a F,
    scalars: Vec<Scalar>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    /// Create a new finder with the `test_fn`
    #[allow(dead_code)]
    fn new(find_fn: &'a F) -> Self {
        Self {
            find_fn,
            scalars: Vec::new(),
        }
    }
}

impl<'a, F> ScalarVisitor for Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    fn pre_visit(mut self, scalar: &Scalar) -> Result<Recursion<Self>> {
        if (self.find_fn)(scalar) {
            if !(self.scalars.contains(scalar)) {
                self.scalars.push((*scalar).clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

pub fn split_conjunctions(scalar: &Scalar) -> Vec<Scalar> {
    match scalar {
        Scalar::AndExpr(AndExpr { left, right, .. }) => {
            vec![split_conjunctions(left), split_conjunctions(right)].concat()
        }
        _ => {
            vec![scalar.clone()]
        }
    }
}

pub fn split_equivalent_predicate(scalar: &Scalar) -> Option<(Scalar, Scalar)> {
    match scalar {
        Scalar::ComparisonExpr(ComparisonExpr {
            op, left, right, ..
        }) if *op == ComparisonOp::Equal => Some((*left.clone(), *right.clone())),
        _ => None,
    }
}

pub fn satisfied_by(scalar: &Scalar, prop: &RelationalProperty) -> bool {
    scalar.used_columns().is_subset(&prop.output_columns)
}

/// Helper to determine join condition type from a scalar expression.
/// Given a query: `SELECT * FROM t(a), t1(b) WHERE a = 1 AND b = 1 AND a = b AND a+b = 1`,
/// the predicate types are:
/// - Left: `a = 1`
/// - Right: `b = 1`
/// - Both: `a = b`
/// - Other: `a+b = 1`
#[derive(Clone, Debug)]
pub enum JoinPredicate<'a> {
    Left(&'a Scalar),
    Right(&'a Scalar),
    Both { left: &'a Scalar, right: &'a Scalar },
    Other(&'a Scalar),
}

impl<'a> JoinPredicate<'a> {
    pub fn new(
        scalar: &'a Scalar,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
    ) -> Self {
        if contain_subquery(scalar) {
            return Self::Other(scalar);
        }
        if satisfied_by(scalar, left_prop) {
            return Self::Left(scalar);
        }

        if satisfied_by(scalar, right_prop) {
            return Self::Right(scalar);
        }

        if let Scalar::ComparisonExpr(ComparisonExpr {
            op: ComparisonOp::Equal,
            left,
            right,
            ..
        }) = scalar
        {
            if satisfied_by(left, left_prop) && satisfied_by(right, right_prop) {
                return Self::Both { left, right };
            }

            if satisfied_by(right, left_prop) && satisfied_by(left, right_prop) {
                return Self::Both {
                    left: right,
                    right: left,
                };
            }
        }

        Self::Other(scalar)
    }
}

pub fn contain_subquery(scalar: &Scalar) -> bool {
    match scalar {
        Scalar::BoundColumnRef(BoundColumnRef { column }) => {
            // For example: SELECT * FROM c WHERE c_id=(SELECT c_id FROM o WHERE ship='WA' AND bill='FL');
            // predicate `c_id = scalar_subquery_{}` can't be pushed down to the join condition.
            // TODO(xudong963): need a better way to handle this, such as add a field to predicate to indicate if it derives from subquery.
            column.column_name == format!("scalar_subquery_{}", column.index)
        }
        Scalar::ComparisonExpr(ComparisonExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        Scalar::AndExpr(AndExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        Scalar::OrExpr(OrExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        Scalar::NotExpr(NotExpr { argument, .. }) => contain_subquery(argument),
        Scalar::FunctionCall(FunctionCall { arguments, .. }) => {
            arguments.iter().any(contain_subquery)
        }
        Scalar::CastExpr(CastExpr { argument, .. }) => contain_subquery(argument),
        _ => false,
    }
}

/// Wrap a cast expression with given target type
pub fn wrap_cast(scalar: &Scalar, target_type: &DataType) -> Scalar {
    Scalar::CastExpr(CastExpr {
        argument: Box::new(scalar.clone()),
        from_type: Box::new(scalar.data_type()),
        target_type: Box::new(target_type.clone()),
    })
}

/// Wrap a cast expression with given target type if the scalar is not of the target type
pub fn wrap_cast_if_needed(scalar: &Scalar, target_type: &DataType) -> Scalar {
    if &scalar.data_type() == target_type {
        scalar.clone()
    } else {
        wrap_cast(scalar, target_type)
    }
}
