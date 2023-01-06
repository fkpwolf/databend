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
use common_expression::type_check;
use common_expression::Expr;
use common_expression::RawExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use crate::plans::Scalar;
use crate::ScalarExpr;

const DUMMY_NAME: &str = "DUMMY";

impl Scalar {
    /// Lowering `Scalar` into `RawExpr` to utilize with `common_expression::types::type_check`.
    /// Specific variants will be replaced with a `RawExpr::ColumnRef` with a dummy name.
    pub fn as_raw_expr(&self) -> RawExpr<String> {
        match self {
            Scalar::BoundColumnRef(column_ref) => RawExpr::ColumnRef {
                span: None,
                id: column_ref.column.column_name.clone(),
                data_type: *column_ref.column.data_type.clone(),
            },
            Scalar::ConstantExpr(constant) => RawExpr::Literal {
                span: None,
                lit: constant.value.clone(),
            },
            Scalar::AndExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "and".to_string(),
                params: vec![],
                args: vec![expr.left.as_raw_expr(), expr.right.as_raw_expr()],
            },
            Scalar::OrExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "or".to_string(),
                params: vec![],
                args: vec![expr.left.as_raw_expr(), expr.right.as_raw_expr()],
            },
            Scalar::ComparisonExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: expr.op.to_func_name(),
                params: vec![],
                args: vec![expr.left.as_raw_expr(), expr.right.as_raw_expr()],
            },
            Scalar::AggregateFunction(agg) => RawExpr::ColumnRef {
                span: None,
                id: agg.display_name.clone(),
                data_type: *agg.return_type.clone(),
            },
            Scalar::FunctionCall(func) => RawExpr::FunctionCall {
                span: None,
                name: func.func_name.clone(),
                params: vec![],
                args: func.arguments.iter().map(Scalar::as_raw_expr).collect(),
            },
            Scalar::CastExpr(cast) => {
                let is_try = cast.target_type.is_nullable();
                RawExpr::Cast {
                    span: None,
                    is_try,
                    expr: Box::new(cast.argument.as_raw_expr()),
                    dest_type: *cast.target_type.clone(),
                }
            }
            Scalar::SubqueryExpr(subquery) => RawExpr::ColumnRef {
                span: None,
                id: DUMMY_NAME.to_string(),
                data_type: subquery.data_type(),
            },
        }
    }

    /// Convert to `Expr<String>` by type checking.
    pub fn as_expr(&self) -> Result<Expr<String>> {
        let raw_expr = self.as_raw_expr();
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).map_err(|(_, e)| {
            common_exception::ErrorCode::Internal(format!(
                "Failed to type check the expression: {raw_expr:?}, error: {e}",
            ))
        })?;
        Ok(expr)
    }
}
