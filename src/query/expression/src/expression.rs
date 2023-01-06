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
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::function::Function;
use crate::function::FunctionID;
use crate::function::FunctionRegistry;
use crate::types::number::NumberScalar;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::DataType;
use crate::values::Scalar;

pub type Span = Option<std::ops::Range<usize>>;

pub trait ColumnIndex: Debug + Clone + Serialize + Hash + Eq {
    fn sql_display_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
    fn sql_display(&self) -> String {
        let mut buf = String::new();
        let mut formatter = std::fmt::Formatter::new(&mut buf);
        self.sql_display_fmt(&mut formatter).unwrap();
        buf
    }
}

impl ColumnIndex for usize {
    fn sql_display_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self)
    }
}

impl ColumnIndex for String {
    fn sql_display_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, Clone)]
pub enum RawExpr<Index: ColumnIndex = usize> {
    Literal {
        span: Span,
        lit: Literal,
    },
    ColumnRef {
        span: Span,
        id: Index,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<RawExpr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        name: String,
        params: Vec<usize>,
        args: Vec<RawExpr<Index>>,
    },
}

#[derive(Debug, Clone, Educe, EnumAsInner)]
#[educe(PartialEq)]
pub enum Expr<Index: ColumnIndex = usize> {
    Constant {
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        span: Span,
        id: Index,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<Expr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        id: FunctionID,
        #[educe(PartialEq(ignore))]
        function: Arc<Function>,
        generics: Vec<DataType>,
        args: Vec<Expr<Index>>,
        return_type: DataType,
    },
}

/// Serializable expression used to share executable expression between nodes.
///
/// The remote node will recover the `Arc` pointer within `FunctionCall` by looking
/// up the funciton registry with the `FunctionID`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteExpr<Index: ColumnIndex = usize> {
    Constant {
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        span: Span,
        id: Index,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<RemoteExpr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        id: FunctionID,
        generics: Vec<DataType>,
        args: Vec<RemoteExpr<Index>>,
        return_type: DataType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum Literal {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(F32),
    Float64(F64),
    Boolean(bool),
    String(Vec<u8>),
}

impl Literal {
    pub fn into_scalar(self) -> Scalar {
        match self {
            Literal::Null => Scalar::Null,
            Literal::Int8(value) => Scalar::Number(NumberScalar::Int8(value)),
            Literal::Int16(value) => Scalar::Number(NumberScalar::Int16(value)),
            Literal::Int32(value) => Scalar::Number(NumberScalar::Int32(value)),
            Literal::Int64(value) => Scalar::Number(NumberScalar::Int64(value)),
            Literal::UInt8(value) => Scalar::Number(NumberScalar::UInt8(value)),
            Literal::UInt16(value) => Scalar::Number(NumberScalar::UInt16(value)),
            Literal::UInt32(value) => Scalar::Number(NumberScalar::UInt32(value)),
            Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(value)),
            Literal::Float32(value) => Scalar::Number(NumberScalar::Float32(value)),
            Literal::Float64(value) => Scalar::Number(NumberScalar::Float64(value)),
            Literal::Boolean(value) => Scalar::Boolean(value),
            Literal::String(value) => Scalar::String(value.to_vec()),
        }
    }
}

impl TryFrom<Scalar> for Literal {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        match value {
            Scalar::Null => Ok(Literal::Null),
            Scalar::Number(NumberScalar::Int8(value)) => Ok(Literal::Int8(value)),
            Scalar::Number(NumberScalar::Int16(value)) => Ok(Literal::Int16(value)),
            Scalar::Number(NumberScalar::Int32(value)) => Ok(Literal::Int32(value)),
            Scalar::Number(NumberScalar::Int64(value)) => Ok(Literal::Int64(value)),
            Scalar::Number(NumberScalar::UInt8(value)) => Ok(Literal::UInt8(value)),
            Scalar::Number(NumberScalar::UInt16(value)) => Ok(Literal::UInt16(value)),
            Scalar::Number(NumberScalar::UInt32(value)) => Ok(Literal::UInt32(value)),
            Scalar::Number(NumberScalar::UInt64(value)) => Ok(Literal::UInt64(value)),
            Scalar::Number(NumberScalar::Float32(value)) => Ok(Literal::Float32(value)),
            Scalar::Number(NumberScalar::Float64(value)) => Ok(Literal::Float64(value)),
            Scalar::Boolean(value) => Ok(Literal::Boolean(value)),
            Scalar::String(value) => Ok(Literal::String(value.to_vec())),
            _ => Err(ErrorCode::Internal("Unsupported scalar value")),
        }
    }
}

impl<Index: ColumnIndex> RawExpr<Index> {
    pub fn column_refs(&self) -> HashMap<Index, DataType> {
        fn walk<Index: ColumnIndex>(expr: &RawExpr<Index>, buf: &mut HashMap<Index, DataType>) {
            match expr {
                RawExpr::ColumnRef { id, data_type, .. } => {
                    buf.insert(id.clone(), data_type.clone());
                }
                RawExpr::Cast { expr, .. } => walk(expr, buf),
                RawExpr::FunctionCall { args, .. } => args.iter().for_each(|expr| walk(expr, buf)),
                RawExpr::Literal { .. } => (),
            }
        }

        let mut buf = HashMap::new();
        walk(self, &mut buf);
        buf
    }
}

impl<Index: ColumnIndex> Expr<Index> {
    pub fn data_type(&self) -> &DataType {
        match self {
            Expr::Constant { data_type, .. } => data_type,
            Expr::ColumnRef { data_type, .. } => data_type,
            Expr::Cast { dest_type, .. } => dest_type,
            Expr::FunctionCall { return_type, .. } => return_type,
        }
    }

    pub fn column_refs(&self) -> HashMap<Index, DataType> {
        fn walk<Index: ColumnIndex>(expr: &Expr<Index>, buf: &mut HashMap<Index, DataType>) {
            match expr {
                Expr::ColumnRef { id, data_type, .. } => {
                    buf.insert(id.clone(), data_type.clone());
                }
                Expr::Cast { expr, .. } => walk(expr, buf),
                Expr::FunctionCall { args, .. } => args.iter().for_each(|expr| walk(expr, buf)),
                Expr::Constant { .. } => (),
            }
        }

        let mut buf = HashMap::new();
        walk(self, &mut buf);
        buf
    }

    pub fn sql_display(&self) -> String {
        match self {
            Expr::Constant { scalar, .. } => format!("{}", scalar.as_ref()),
            Expr::ColumnRef { id, .. } => id.sql_display(),
            Expr::Cast {
                is_try,
                expr,
                dest_type,
                ..
            } => {
                if *is_try {
                    format!("TRY_CAST({} AS {dest_type})", expr.sql_display())
                } else {
                    format!("CAST({} AS {dest_type})", expr.sql_display())
                }
            }
            Expr::FunctionCall { function, args, .. } => {
                let mut s = String::new();
                s += &function.signature.name;
                s += "(";
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        s += ", ";
                    }
                    s += &arg.sql_display();
                }
                s += ")";
                s
            }
        }
    }

    pub fn project_column_ref<ToIndex: ColumnIndex>(
        &self,
        f: impl Fn(&Index) -> ToIndex + Copy,
    ) -> Expr<ToIndex> {
        match self {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span: span.clone(),
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
            } => Expr::ColumnRef {
                span: span.clone(),
                id: f(id),
                data_type: data_type.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: span.clone(),
                is_try: *is_try,
                expr: Box::new(expr.project_column_ref(f)),
                dest_type: dest_type.clone(),
            },
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            } => Expr::FunctionCall {
                span: span.clone(),
                id: id.clone(),
                function: function.clone(),
                generics: generics.clone(),
                args: args.iter().map(|expr| expr.project_column_ref(f)).collect(),
                return_type: return_type.clone(),
            },
        }
    }

    pub fn as_remote_expr(&self) -> RemoteExpr<Index> {
        match self {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => RemoteExpr::Constant {
                span: span.clone(),
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
            } => RemoteExpr::ColumnRef {
                span: span.clone(),
                id: id.clone(),
                data_type: data_type.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => RemoteExpr::Cast {
                span: span.clone(),
                is_try: *is_try,
                expr: Box::new(expr.as_remote_expr()),
                dest_type: dest_type.clone(),
            },
            Expr::FunctionCall {
                span,
                id,
                function: _,
                generics,
                args,
                return_type,
            } => RemoteExpr::FunctionCall {
                span: span.clone(),
                id: id.clone(),
                generics: generics.clone(),
                args: args.iter().map(Expr::as_remote_expr).collect(),
                return_type: return_type.clone(),
            },
        }
    }
}

impl<Index: ColumnIndex> RemoteExpr<Index> {
    pub fn as_expr(&self, fn_registry: &FunctionRegistry) -> Option<Expr<Index>> {
        Some(match self {
            RemoteExpr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span: span.clone(),
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
            } => Expr::ColumnRef {
                span: span.clone(),
                id: id.clone(),
                data_type: data_type.clone(),
            },
            RemoteExpr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: span.clone(),
                is_try: *is_try,
                expr: Box::new(expr.as_expr(fn_registry)?),
                dest_type: dest_type.clone(),
            },
            RemoteExpr::FunctionCall {
                span,
                id,
                generics,
                args,
                return_type,
            } => {
                let function = fn_registry.get(id)?;
                Expr::FunctionCall {
                    span: span.clone(),
                    id: id.clone(),
                    function,
                    generics: generics.clone(),
                    args: args
                        .iter()
                        .map(|arg| arg.as_expr(fn_registry))
                        .collect::<Option<_>>()?,
                    return_type: return_type.clone(),
                }
            }
        })
    }
}
