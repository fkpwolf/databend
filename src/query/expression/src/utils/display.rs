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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use comfy_table::Cell;
use comfy_table::Table;
use itertools::Itertools;
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

use crate::block::DataBlock;
use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::Function;
use crate::function::FunctionSignature;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::types::boolean::BooleanDomain;
use crate::types::date::date_to_string;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberDataType;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::string::StringColumn;
use crate::types::string::StringDomain;
use crate::types::timestamp::timestamp_to_string;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::ValueType;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::values::Value;
use crate::values::ValueRef;
use crate::with_number_type;
use crate::Column;
use crate::ColumnIndex;
use crate::TableDataType;

const FLOAT_NUM_FRAC_DIGITS: u32 = 10;

impl Debug for DataBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header(vec!["Column ID", "Type", "Column Data"]);

        for (i, entry) in self.columns().iter().enumerate() {
            table.add_row(vec![
                i.to_string(),
                entry.data_type.to_string(),
                format!("{:?}", entry.value),
            ]);
        }

        write!(f, "{}", table)
    }
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        table.set_header((0..self.num_columns()).map(|idx| format!("Column {idx}")));

        for index in 0..self.num_rows() {
            let row: Vec<_> = self
                .columns()
                .iter()
                .map(|entry| entry.value.as_ref().index(index).unwrap().to_string())
                .map(Cell::new)
                .collect();
            table.add_row(row);
        }
        write!(f, "{table}")
    }
}

impl<'a> Debug for ScalarRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[] :: Array(Nothing)"),
            ScalarRef::Number(val) => write!(f, "{val:?}"),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::String(s) => write!(f, "{:?}", String::from_utf8_lossy(s)),
            ScalarRef::Timestamp(t) => write!(f, "{t:?}"),
            ScalarRef::Date(d) => write!(f, "{d:?}"),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Tuple(fields) => {
                write!(f, "(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field:?}")?;
                }
                if fields.len() < 2 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            ScalarRef::Variant(s) => write!(f, "0x{}", &hex::encode(s)),
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Null { len } => f.debug_struct("Null").field("len", len).finish(),
            Column::EmptyArray { len } => f.debug_struct("EmptyArray").field("len", len).finish(),
            Column::Number(col) => write!(f, "{col:?}"),
            Column::Boolean(col) => f.debug_tuple("Boolean").field(col).finish(),
            Column::String(col) => write!(f, "{col:?}"),
            Column::Timestamp(col) => write!(f, "{col:?}"),
            Column::Date(col) => write!(f, "{col:?}"),
            Column::Array(col) => write!(f, "{col:?}"),
            Column::Nullable(col) => write!(f, "{col:?}"),
            Column::Tuple { fields, len } => f
                .debug_struct("Tuple")
                .field("fields", fields)
                .field("len", len)
                .finish(),
            Column::Variant(col) => write!(f, "{col:?}"),
        }
    }
}

impl<'a> Display for ScalarRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarRef::Null => write!(f, "NULL"),
            ScalarRef::EmptyArray => write!(f, "[]"),
            ScalarRef::Number(val) => write!(f, "{:?}", val),
            ScalarRef::Boolean(val) => write!(f, "{val}"),
            ScalarRef::String(s) => write!(f, "{:?}", String::from_utf8_lossy(s)),
            ScalarRef::Timestamp(t) => write!(f, "{}", timestamp_to_string(*t, chrono_tz::Tz::UTC)),
            ScalarRef::Date(d) => write!(f, "{}", date_to_string(*d as i64, chrono_tz::Tz::UTC)),
            ScalarRef::Array(col) => write!(f, "[{}]", col.iter().join(", ")),
            ScalarRef::Tuple(fields) => {
                write!(f, "(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                if fields.len() < 2 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            ScalarRef::Variant(s) => {
                let value = common_jsonb::to_string(s);
                write!(f, "{value}")
            }
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Scalar::Null => write!(f, "NULL"),
            Scalar::EmptyArray => write!(f, "[]"),
            Scalar::Number(n) => write!(f, "{}", n),
            Scalar::Boolean(b) => write!(f, "{}", b),
            Scalar::String(s) => match std::str::from_utf8(s) {
                Ok(v) => write!(f, "{}", v),
                Err(_e) => {
                    for c in s {
                        write!(f, "{:02x}", c)?;
                    }
                    Ok(())
                }
            },
            Scalar::Timestamp(t) => write!(f, "{}", timestamp_to_string(*t, chrono_tz::Tz::UTC)),
            Scalar::Date(d) => write!(f, "{}", date_to_string(*d as i64, chrono_tz::Tz::UTC)),
            Scalar::Array(v) => {
                write!(
                    f,
                    "[{}]",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Scalar::Tuple(v) => {
                write!(
                    f,
                    "({})",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Scalar::Variant(v) => write!(f, "{}", common_jsonb::to_string(v)),
        }
    }
}

impl Debug for NumberScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}_u8"),
            NumberScalar::UInt16(val) => write!(f, "{val}_u16"),
            NumberScalar::UInt32(val) => write!(f, "{val}_u32"),
            NumberScalar::UInt64(val) => write!(f, "{val}_u64"),
            NumberScalar::Int8(val) => write!(f, "{val}_i8"),
            NumberScalar::Int16(val) => write!(f, "{val}_i16"),
            NumberScalar::Int32(val) => write!(f, "{val}_i32"),
            NumberScalar::Int64(val) => write!(f, "{val}_i64"),
            NumberScalar::Float32(val) => match Decimal::from_f32(val.0) {
                Some(d) => write!(
                    f,
                    "{}_f32",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}_f32"),
            },
            NumberScalar::Float64(val) => match Decimal::from_f64(val.0) {
                Some(d) => write!(
                    f,
                    "{}_f64",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}_f64"),
            },
        }
    }
}

impl Display for NumberScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberScalar::UInt8(val) => write!(f, "{val}"),
            NumberScalar::UInt16(val) => write!(f, "{val}"),
            NumberScalar::UInt32(val) => write!(f, "{val}"),
            NumberScalar::UInt64(val) => write!(f, "{val}"),
            NumberScalar::Int8(val) => write!(f, "{val}"),
            NumberScalar::Int16(val) => write!(f, "{val}"),
            NumberScalar::Int32(val) => write!(f, "{val}"),
            NumberScalar::Int64(val) => write!(f, "{val}"),
            NumberScalar::Float32(val) => match Decimal::from_f32(val.0) {
                Some(d) => write!(
                    f,
                    "{}",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}"),
            },
            NumberScalar::Float64(val) => match Decimal::from_f64(val.0) {
                Some(d) => write!(
                    f,
                    "{}",
                    d.round_dp_with_strategy(FLOAT_NUM_FRAC_DIGITS, RoundingStrategy::ToZero)
                        .normalize()
                ),
                None => write!(f, "{val}"),
            },
        }
    }
}

impl Debug for NumberColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberColumn::UInt8(val) => f.debug_tuple("UInt8").field(val).finish(),
            NumberColumn::UInt16(val) => f.debug_tuple("UInt16").field(val).finish(),
            NumberColumn::UInt32(val) => f.debug_tuple("UInt32").field(val).finish(),
            NumberColumn::UInt64(val) => f.debug_tuple("UInt64").field(val).finish(),
            NumberColumn::Int8(val) => f.debug_tuple("Int8").field(val).finish(),
            NumberColumn::Int16(val) => f.debug_tuple("Int16").field(val).finish(),
            NumberColumn::Int32(val) => f.debug_tuple("Int32").field(val).finish(),
            NumberColumn::Int64(val) => f.debug_tuple("Int64").field(val).finish(),
            NumberColumn::Float32(val) => f
                .debug_tuple("Float32")
                .field(&format_args!(
                    "[{}]",
                    &val.iter()
                        .map(|x| match Decimal::from_f32(x.0) {
                            Some(d) => d
                                .round_dp_with_strategy(
                                    FLOAT_NUM_FRAC_DIGITS,
                                    RoundingStrategy::ToZero
                                )
                                .normalize()
                                .to_string(),
                            None => x.to_string(),
                        })
                        .join(", ")
                ))
                .finish(),
            NumberColumn::Float64(val) => f
                .debug_tuple("Float64")
                .field(&format_args!(
                    "[{}]",
                    &val.iter()
                        .map(|x| match Decimal::from_f64(x.0) {
                            Some(d) => d
                                .round_dp_with_strategy(
                                    FLOAT_NUM_FRAC_DIGITS,
                                    RoundingStrategy::ToZero
                                )
                                .normalize()
                                .to_string(),
                            None => x.to_string(),
                        })
                        .join(", ")
                ))
                .finish(),
        }
    }
}

impl Debug for StringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringColumn")
            .field("data", &format_args!("0x{}", &hex::encode(&*self.data)))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl<Index: ColumnIndex> Display for RawExpr<Index> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawExpr::Literal { lit, .. } => write!(f, "{lit}"),
            RawExpr::ColumnRef { id, data_type, .. } => {
                id.sql_display_fmt(f)?;
                write!(f, "::{data_type}")
            }
            RawExpr::Cast {
                is_try,
                expr,
                dest_type,
                ..
            } => {
                if *is_try {
                    write!(f, "TRY_CAST({expr} AS {dest_type})")
                } else {
                    write!(f, "CAST({expr} AS {dest_type})")
                }
            }
            RawExpr::FunctionCall {
                name, args, params, ..
            } => {
                write!(f, "{name}")?;
                if !params.is_empty() {
                    write!(f, "(")?;
                    for (i, param) in params.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{param}")?;
                    }
                    write!(f, ")")?;
                }
                write!(f, "(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(val) => write!(f, "{val}"),
            Literal::Int8(val) => write!(f, "{val}_i8"),
            Literal::Int16(val) => write!(f, "{val}_i16"),
            Literal::Int32(val) => write!(f, "{val}_i32"),
            Literal::Int64(val) => write!(f, "{val}_i64"),
            Literal::UInt8(val) => write!(f, "{val}_u8"),
            Literal::UInt16(val) => write!(f, "{val}_u16"),
            Literal::UInt32(val) => write!(f, "{val}_u32"),
            Literal::UInt64(val) => write!(f, "{val}_u64"),
            Literal::Float32(val) => write!(f, "{val}_f32"),
            Literal::Float64(val) => write!(f, "{val}_f64"),
            Literal::String(val) => write!(f, "{:?}", String::from_utf8_lossy(val)),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::String => write!(f, "String"),
            DataType::Number(num) => write!(f, "{num}"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Date => write!(f, "Date"),
            DataType::Null => write!(f, "NULL"),
            DataType::Nullable(inner) => write!(f, "{inner} NULL"),
            DataType::EmptyArray => write!(f, "Array(Nothing)"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
            DataType::Map(inner) => write!(f, "Map({inner})"),
            DataType::Tuple(tys) => {
                write!(f, "(")?;
                for (i, ty) in tys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{ty}")?;
                }
                if tys.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            DataType::Variant => write!(f, "Variant"),
            DataType::Generic(index) => write!(f, "T{index}"),
        }
    }
}

impl Display for TableDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            TableDataType::Boolean => write!(f, "Boolean"),
            TableDataType::String => write!(f, "String"),
            TableDataType::Number(num) => write!(f, "{num}"),
            TableDataType::Timestamp => write!(f, "Timestamp"),
            TableDataType::Date => write!(f, "Date"),
            TableDataType::Null => write!(f, "NULL"),
            TableDataType::Nullable(inner) => write!(f, "{inner} NULL"),
            TableDataType::EmptyArray => write!(f, "Array(Nothing)"),
            TableDataType::Array(inner) => write!(f, "Array({inner})"),
            TableDataType::Map(inner) => write!(f, "Map({inner})"),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                write!(f, "(")?;
                for (i, (name, ty)) in fields_name.iter().zip(fields_type).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{name} {ty}")?;
                }
                if fields_name.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            TableDataType::Variant => write!(f, "Variant"),
        }
    }
}

impl Display for NumberDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            NumberDataType::UInt8 => write!(f, "UInt8"),
            NumberDataType::UInt16 => write!(f, "UInt16"),
            NumberDataType::UInt32 => write!(f, "UInt32"),
            NumberDataType::UInt64 => write!(f, "UInt64"),
            NumberDataType::Int8 => write!(f, "Int8"),
            NumberDataType::Int16 => write!(f, "Int16"),
            NumberDataType::Int32 => write!(f, "Int32"),
            NumberDataType::Int64 => write!(f, "Int64"),
            NumberDataType::Float32 => write!(f, "Float32"),
            NumberDataType::Float64 => write!(f, "Float64"),
        }
    }
}

impl<Index: ColumnIndex> Display for Expr<Index> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Constant { scalar, .. } => write!(f, "{:?}", scalar.as_ref()),
            Expr::ColumnRef { id, .. } => id.sql_display_fmt(f),
            Expr::Cast {
                is_try,
                expr,
                dest_type,
                ..
            } => {
                if *is_try {
                    write!(f, "TRY_CAST({expr} AS {dest_type})")
                } else {
                    write!(f, "CAST({expr} AS {dest_type})")
                }
            }
            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } => {
                write!(f, "{}", function.signature.name)?;
                if !generics.is_empty() {
                    write!(f, "<")?;
                    for (i, ty) in generics.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "T{i}={ty}")?;
                    }
                    write!(f, ">")?;
                }
                write!(f, "<")?;
                for (i, ty) in function.signature.args_type.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{ty}")?;
                }
                write!(f, ">")?;
                write!(f, "(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
        }
    }
}

impl<T: ValueType> Display for Value<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Value::Scalar(scalar) => write!(f, "{:?}", scalar),
            Value::Column(col) => write!(f, "{:?}", col),
        }
    }
}

impl<'a, T: ValueType> Display for ValueRef<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ValueRef::Scalar(scalar) => write!(f, "{:?}", scalar),
            ValueRef::Column(col) => write!(f, "{:?}", col),
        }
    }
}

impl Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.signature)
    }
}

impl Display for FunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}) :: {}",
            self.name,
            self.args_type.iter().map(|t| t.to_string()).join(", "),
            self.return_type
        )
    }
}

impl Display for FunctionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut properties = Vec::new();
        if self.non_deterministic {
            properties.push("non_deterministic");
        }
        if !properties.is_empty() {
            write!(f, "{{{}}}", properties.join(", "))?;
        }
        Ok(())
    }
}

impl Display for NullableDomain<AnyType> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(value) = &self.value {
            if self.has_null {
                write!(f, "{} ∪ {{NULL}}", value)
            } else {
                write!(f, "{}", value)
            }
        } else {
            assert!(self.has_null);
            write!(f, "{{NULL}}")
        }
    }
}

impl Display for BooleanDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_false && self.has_true {
            write!(f, "{{FALSE, TRUE}}")
        } else if self.has_false {
            write!(f, "{{FALSE}}")
        } else {
            write!(f, "{{TRUE}}")
        }
    }
}

impl Display for StringDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(max) = &self.max {
            write!(
                f,
                "{{{:?}..={:?}}}",
                String::from_utf8_lossy(&self.min),
                String::from_utf8_lossy(max)
            )
        } else {
            write!(f, "{{{:?}..}}", String::from_utf8_lossy(&self.min))
        }
    }
}

impl Display for NumberDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        with_number_type!(|TYPE| match self {
            NumberDomain::TYPE(domain) => write!(f, "{domain}"),
        })
    }
}

impl<T: Display> Display for SimpleDomain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{{}..={}}}", self.min, self.max)
    }
}
impl Display for Domain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Domain::Number(domain) => write!(f, "{domain}"),
            Domain::Boolean(domain) => write!(f, "{domain}"),
            Domain::String(domain) => write!(f, "{domain}"),
            Domain::Timestamp(domain) => write!(f, "{domain}"),
            Domain::Date(domain) => write!(f, "{domain}"),
            Domain::Nullable(domain) => write!(f, "{domain}"),
            Domain::Array(None) => write!(f, "[]"),
            Domain::Array(Some(domain)) => write!(f, "[{domain}]"),
            Domain::Tuple(fields) => {
                write!(f, "(")?;
                for (i, domain) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{domain}")?;
                }
                write!(f, ")")
            }
            Domain::Undefined => write!(f, "Undefined"),
        }
    }
}
