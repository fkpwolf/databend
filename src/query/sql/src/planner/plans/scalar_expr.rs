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

use std::hash::Hash;
use std::hash::Hasher;

use common_ast::ast::BinaryOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::Literal;
use educe::Educe;

use crate::binder::ColumnBinding;
use crate::binder::InternalColumnBinding;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScalarExpr {
    BoundColumnRef(BoundColumnRef),
    BoundInternalColumnRef(BoundInternalColumnRef),
    ConstantExpr(ConstantExpr),
    AndExpr(AndExpr),
    OrExpr(OrExpr),
    NotExpr(NotExpr),
    ComparisonExpr(ComparisonExpr),
    WindowFunction(WindowFunc),
    AggregateFunction(AggregateFunction),
    FunctionCall(FunctionCall),
    Unnest(Unnest),
    // TODO(leiysky): maybe we don't need this variant any more
    // after making functions static typed?
    CastExpr(CastExpr),
    SubqueryExpr(SubqueryExpr),
}

impl ScalarExpr {
    pub fn data_type(&self) -> Result<DataType> {
        Ok(self.as_expr_with_col_index()?.data_type().clone())
    }

    pub fn used_columns(&self) -> ColumnSet {
        match self {
            ScalarExpr::BoundColumnRef(scalar) => ColumnSet::from([scalar.column.index]),
            ScalarExpr::BoundInternalColumnRef(scalar) => ColumnSet::from([scalar.column.index]),
            ScalarExpr::ConstantExpr(_) => ColumnSet::new(),
            ScalarExpr::AndExpr(scalar) => {
                let left: ColumnSet = scalar.left.used_columns();
                let right: ColumnSet = scalar.right.used_columns();
                left.union(&right).cloned().collect()
            }
            ScalarExpr::OrExpr(scalar) => {
                let left: ColumnSet = scalar.left.used_columns();
                let right: ColumnSet = scalar.right.used_columns();
                left.union(&right).cloned().collect()
            }
            ScalarExpr::NotExpr(scalar) => scalar.argument.used_columns(),
            ScalarExpr::ComparisonExpr(scalar) => {
                let left: ColumnSet = scalar.left.used_columns();
                let right: ColumnSet = scalar.right.used_columns();
                left.union(&right).cloned().collect()
            }
            ScalarExpr::WindowFunction(scalar) => {
                let mut result = ColumnSet::new();
                for scalar in &scalar.agg_func.args {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::AggregateFunction(scalar) => {
                let mut result = ColumnSet::new();
                for scalar in &scalar.args {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::FunctionCall(scalar) => {
                let mut result = ColumnSet::new();
                for scalar in &scalar.arguments {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::CastExpr(scalar) => scalar.argument.used_columns(),
            ScalarExpr::SubqueryExpr(scalar) => scalar.outer_columns.clone(),
            ScalarExpr::Unnest(scalar) => scalar.argument.used_columns(),
        }
    }

    /// Collect all [`ScalarExpr`]s that need to be eval before executing `UNNEST`.
    pub fn collect_before_unnest_scalars(&self, scalars: &mut Vec<Box<ScalarExpr>>) {
        match self {
            ScalarExpr::AndExpr(scalar) => {
                scalar.left.collect_before_unnest_scalars(scalars);
                scalar.right.collect_before_unnest_scalars(scalars);
            }
            ScalarExpr::OrExpr(scalar) => {
                scalar.left.collect_before_unnest_scalars(scalars);
                scalar.right.collect_before_unnest_scalars(scalars);
            }
            ScalarExpr::NotExpr(scalar) => scalar.argument.collect_before_unnest_scalars(scalars),
            ScalarExpr::ComparisonExpr(scalar) => {
                scalar.left.collect_before_unnest_scalars(scalars);
                scalar.right.collect_before_unnest_scalars(scalars);
            }
            ScalarExpr::AggregateFunction(scalar) => {
                for scalar in &scalar.args {
                    scalar.collect_before_unnest_scalars(scalars);
                }
            }
            ScalarExpr::FunctionCall(scalar) => {
                for scalar in &scalar.arguments {
                    scalar.collect_before_unnest_scalars(scalars);
                }
            }
            ScalarExpr::CastExpr(scalar) => scalar.argument.collect_before_unnest_scalars(scalars),
            ScalarExpr::Unnest(scalar) => scalars.push(scalar.argument.clone()),
            _ => {}
        }
    }

    pub fn span(&self) -> Span {
        match self {
            ScalarExpr::BoundColumnRef(expr) => expr.span,
            ScalarExpr::ConstantExpr(expr) => expr.span,
            ScalarExpr::FunctionCall(expr) => expr.span,
            ScalarExpr::CastExpr(expr) => expr.span,
            ScalarExpr::SubqueryExpr(expr) => expr.span,
            _ => None,
        }
    }
}

impl From<BoundColumnRef> for ScalarExpr {
    fn from(v: BoundColumnRef) -> Self {
        Self::BoundColumnRef(v)
    }
}

impl From<BoundInternalColumnRef> for ScalarExpr {
    fn from(v: BoundInternalColumnRef) -> Self {
        Self::BoundInternalColumnRef(v)
    }
}

impl TryFrom<ScalarExpr> for BoundColumnRef {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::BoundColumnRef(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to BoundColumnRef",
            ))
        }
    }
}

impl From<ConstantExpr> for ScalarExpr {
    fn from(v: ConstantExpr) -> Self {
        Self::ConstantExpr(v)
    }
}

impl TryFrom<ScalarExpr> for ConstantExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::ConstantExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to ConstantExpr",
            ))
        }
    }
}

impl From<AndExpr> for ScalarExpr {
    fn from(v: AndExpr) -> Self {
        Self::AndExpr(v)
    }
}

impl TryFrom<ScalarExpr> for AndExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::AndExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to AndExpr"))
        }
    }
}

impl From<OrExpr> for ScalarExpr {
    fn from(v: OrExpr) -> Self {
        Self::OrExpr(v)
    }
}

impl TryFrom<ScalarExpr> for OrExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::OrExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to OrExpr"))
        }
    }
}

impl From<NotExpr> for ScalarExpr {
    fn from(v: NotExpr) -> Self {
        Self::NotExpr(v)
    }
}

impl TryFrom<ScalarExpr> for NotExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::NotExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to NotExpr"))
        }
    }
}

impl From<ComparisonExpr> for ScalarExpr {
    fn from(v: ComparisonExpr) -> Self {
        Self::ComparisonExpr(v)
    }
}

impl TryFrom<ScalarExpr> for ComparisonExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::ComparisonExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to ComparisonExpr",
            ))
        }
    }
}

impl From<AggregateFunction> for ScalarExpr {
    fn from(v: AggregateFunction) -> Self {
        Self::AggregateFunction(v)
    }
}

impl TryFrom<ScalarExpr> for AggregateFunction {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::AggregateFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to AggregateFunction",
            ))
        }
    }
}

impl From<WindowFunc> for ScalarExpr {
    fn from(v: WindowFunc) -> Self {
        Self::WindowFunction(v)
    }
}

impl TryFrom<ScalarExpr> for WindowFunc {
    type Error = ErrorCode;

    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::WindowFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to WindowFunc"))
        }
    }
}

impl From<Unnest> for ScalarExpr {
    fn from(v: Unnest) -> Self {
        Self::Unnest(v)
    }
}

impl TryFrom<ScalarExpr> for Unnest {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::Unnest(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to Unnest"))
        }
    }
}

impl From<FunctionCall> for ScalarExpr {
    fn from(v: FunctionCall) -> Self {
        Self::FunctionCall(v)
    }
}

impl TryFrom<ScalarExpr> for FunctionCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::FunctionCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to FunctionCall",
            ))
        }
    }
}

impl From<CastExpr> for ScalarExpr {
    fn from(v: CastExpr) -> Self {
        Self::CastExpr(v)
    }
}

impl TryFrom<ScalarExpr> for CastExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::CastExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to CastExpr"))
        }
    }
}

impl From<SubqueryExpr> for ScalarExpr {
    fn from(v: SubqueryExpr) -> Self {
        Self::SubqueryExpr(v)
    }
}

impl TryFrom<ScalarExpr> for SubqueryExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::SubqueryExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to SubqueryExpr",
            ))
        }
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct BoundColumnRef {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub column: ColumnBinding,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BoundInternalColumnRef {
    pub column: InternalColumnBinding,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct ConstantExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub value: Literal,
    pub data_type: Box<DataType>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AndExpr {
    pub left: Box<ScalarExpr>,
    pub right: Box<ScalarExpr>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OrExpr {
    pub left: Box<ScalarExpr>,
    pub right: Box<ScalarExpr>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NotExpr {
    pub argument: Box<ScalarExpr>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    // Greater ">"
    GT,
    // Less "<"
    LT,
    // Greater or equal ">="
    GTE,
    // Less or equal "<="
    LTE,
}

impl ComparisonOp {
    pub fn try_from_binary_op(op: &BinaryOperator) -> Result<Self> {
        match op {
            BinaryOperator::Gt => Ok(Self::GT),
            BinaryOperator::Lt => Ok(Self::LT),
            BinaryOperator::Gte => Ok(Self::GTE),
            BinaryOperator::Lte => Ok(Self::LTE),
            BinaryOperator::Eq => Ok(Self::Equal),
            BinaryOperator::NotEq => Ok(Self::NotEqual),
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported comparison operator {op}"
            ))),
        }
    }

    pub fn to_func_name(&self) -> &'static str {
        match &self {
            ComparisonOp::Equal => "eq",
            ComparisonOp::NotEqual => "noteq",
            ComparisonOp::GT => "gt",
            ComparisonOp::LT => "lt",
            ComparisonOp::GTE => "gte",
            ComparisonOp::LTE => "lte",
        }
    }
}

impl<'a> TryFrom<&'a BinaryOperator> for ComparisonOp {
    type Error = ErrorCode;

    fn try_from(value: &'a BinaryOperator) -> Result<Self> {
        Self::try_from_binary_op(value)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ComparisonExpr {
    pub op: ComparisonOp,
    pub left: Box<ScalarExpr>,
    pub right: Box<ScalarExpr>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunction {
    pub func_name: String,
    pub distinct: bool,
    pub params: Vec<Literal>,
    pub args: Vec<ScalarExpr>,
    pub return_type: Box<DataType>,

    pub display_name: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WindowFunc {
    pub agg_func: AggregateFunction,
    pub partition_by: Vec<ScalarExpr>,
    pub frame: WindowFuncFrame,
}

impl WindowFunc {
    pub fn display_name(&self) -> String {
        format!("{}_with_window", self.agg_func.func_name)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WindowFuncFrame {
    pub units: WindowFuncFrameUnits,
    pub start: WindowFuncFrameBound,
    pub end: WindowFuncFrameBound,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WindowFuncFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Box<ScalarExpr>>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Box<ScalarExpr>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFuncFrameUnits {
    Rows,
    Range,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FunctionCall {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub params: Vec<usize>,
    pub arguments: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unnest {
    pub argument: Box<ScalarExpr>,
    pub return_type: Box<DataType>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CastExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub is_try: bool,
    pub argument: Box<ScalarExpr>,
    pub target_type: Box<DataType>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SubqueryType {
    Any,
    All,
    Scalar,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SubqueryExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub typ: SubqueryType,
    pub subquery: Box<SExpr>,
    // The expr that is used to compare the result of the subquery (IN/ANY/ALL), such as `t1.a in (select t2.a from t2)`, t1.a is `child_expr`.
    pub child_expr: Option<Box<ScalarExpr>>,
    // Comparison operator for Any/All, such as t1.a = Any (...), `compare_op` is `=`.
    pub compare_op: Option<ComparisonOp>,
    // Output column of Any/All and scalar subqueries.
    pub output_column: ColumnBinding,
    pub projection_index: Option<IndexType>,
    pub(crate) data_type: Box<DataType>,
    #[educe(Hash(method = "hash_column_set"))]
    pub outer_columns: ColumnSet,
}

impl SubqueryExpr {
    pub fn data_type(&self) -> DataType {
        match &self.typ {
            SubqueryType::Scalar => (*self.data_type).clone(),
            SubqueryType::Any
            | SubqueryType::All
            | SubqueryType::Exists
            | SubqueryType::NotExists => DataType::Nullable(Box::new(DataType::Boolean)),
        }
    }
}

fn hash_column_set<H: Hasher>(columns: &ColumnSet, state: &mut H) {
    columns.iter().for_each(|c| c.hash(state));
}
