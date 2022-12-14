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

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::compute::comparison;
use common_datavalues::prelude::*;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::with_match_physical_primitive_type;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;

use super::utils::*;
use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::new_mutable_bitmap;
use crate::scalars::primitive_simd_op_boolean;
use crate::scalars::scalar_binary_op;
use crate::scalars::ComparisonEqFunction;
use crate::scalars::ComparisonGtEqFunction;
use crate::scalars::ComparisonGtFunction;
use crate::scalars::ComparisonLikeFunction;
use crate::scalars::ComparisonLtEqFunction;
use crate::scalars::ComparisonLtFunction;
use crate::scalars::ComparisonNotEqFunction;
use crate::scalars::ComparisonNotLikeFunction;
use crate::scalars::ComparisonNotRegexpFunction;
use crate::scalars::ComparisonRegexpFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFactory;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ComparisonFunction {
    display_name: String,
    func: Arc<dyn ComparisonExpression>,
}

impl ComparisonFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("=", ComparisonEqFunction::desc("<>"));
        factory.register("<", ComparisonLtFunction::desc(">="));
        factory.register(">", ComparisonGtFunction::desc("<="));
        factory.register("<=", ComparisonLtEqFunction::desc(">"));
        factory.register(">=", ComparisonGtEqFunction::desc("<"));
        factory.register("!=", ComparisonNotEqFunction::desc("="));
        factory.register("<>", ComparisonNotEqFunction::desc("="));
        factory.register("like", ComparisonLikeFunction::desc("not like"));
        factory.register("not like", ComparisonNotLikeFunction::desc("like"));
        factory.register("regexp", ComparisonRegexpFunction::desc("not regexp"));
        factory.register("not regexp", ComparisonNotRegexpFunction::desc("regexp"));
        factory.register("rlike", ComparisonRegexpFunction::desc("not regexp"));
        factory.register("not rlike", ComparisonNotRegexpFunction::desc("regexp"));
    }

    pub fn try_create_func(
        display_name: &str,
        func: Arc<dyn ComparisonExpression>,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            func,
        }))
    }
}

impl Function for ComparisonFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        BooleanType::new_impl()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        let col = self.func.eval(&columns[0], &columns[1])?;
        Ok(Arc::new(col))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub struct ComparisonFunctionCreator<T> {
    t: PhantomData<T>,
}

impl<T: ComparisonImpl> ComparisonFunctionCreator<T> {
    pub fn try_create_func(
        display_name: &str,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        // expect array & struct
        let has_array_struct = args
            .iter()
            .any(|arg| matches!(arg.data_type_id(), TypeID::Struct | TypeID::Array));

        if has_array_struct {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal types {:?} of argument of function {}, can not be struct or array",
                args, display_name
            )));
        }

        let lhs_id = args[0].data_type_id();
        let rhs_id = args[1].data_type_id();

        if lhs_id.is_variant() && rhs_id.is_variant() {
            let func = Arc::new(ComparisonScalarImpl::<VariantValue, VariantValue, _>::new(
                args[0].clone(),
                false,
                T::eval_variant,
            ));
            return ComparisonFunction::try_create_func(display_name, func);
        }

        if args[0].eq(args[1]) {
            return with_match_physical_primitive_type!(lhs_id.to_physical_type(), |$T| {
                let func = Arc::new(ComparisonPrimitiveImpl::<$T, _>::new(args[0].clone(), false, T::eval_simd::<$T>));
                ComparisonFunction::try_create_func(display_name, func)
            }, {
                match lhs_id {
                    TypeID::Boolean => {
                        let func = Arc::new(ComparisonBooleanImpl::<T::BooleanSimd>::new());
                        ComparisonFunction::try_create_func(display_name, func)
                    },
                    TypeID::String => {
                        let func = Arc::new(ComparisonScalarImpl::<Vu8, Vu8, _>::new(args[0].clone(), false, T::eval_binary));
                        ComparisonFunction::try_create_func(display_name, func)
                    },
                    _ => Err(ErrorCode::IllegalDataType(format!(
                        "Can not compare {:?} with {:?}",
                        args[0], args[1]
                    ))),
                }
            });
        }

        if lhs_id.is_numeric() && rhs_id.is_numeric() {
            return with_match_primitive_types_error!(lhs_id, |$T| {
                with_match_primitive_types_error!(rhs_id, |$D| {
                    let func = Arc::new(ComparisonScalarImpl::<$T, $D, _>::new(args[0].clone(), false, T::eval_primitive::<$T, $D, <($T, $D) as ResultTypeOfBinary>::LeastSuper>));
                    ComparisonFunction::try_create_func(display_name, func)
                })
            });
        }

        if (lhs_id.is_string() && rhs_id.is_numeric())
            || (rhs_id.is_string() && lhs_id.is_numeric())
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Can not compare {:?} with {:?}",
                args[0].data_type_id(),
                args[1].data_type_id()
            )));
        }

        let least_supertype = compare_coercion(args[0], args[1])?;

        with_match_physical_primitive_type!(least_supertype.data_type_id().to_physical_type(), |$T| {
            let func = Arc::new(ComparisonPrimitiveImpl::<$T, _>::new(least_supertype, true, T::eval_simd::<$T>));
            ComparisonFunction::try_create_func(display_name, func)
        }, {
            match least_supertype.data_type_id() {
                TypeID::Boolean => {
                    let func = Arc::new(ComparisonBooleanImpl::<T::BooleanSimd>::new());
                    ComparisonFunction::try_create_func(display_name, func)
                },
                TypeID::String => {
                    let func = Arc::new(ComparisonScalarImpl::<Vu8, Vu8, _>::new(least_supertype, true, T::eval_binary));
                    ComparisonFunction::try_create_func(display_name, func)
                },
                _ => Err(ErrorCode::IllegalDataType(format!(
                    "Can not compare {:?} with {:?}",
                    args[0].data_type_id(), args[1].data_type_id()
                ))),
            }
        })
    }

    pub fn desc(negative_name: &str) -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function(negative_name)
                .num_arguments(2),
        )
    }
}

pub struct StringSearchCreator<const NEGATED: bool, T> {
    t: PhantomData<T>,
}

impl<const NEGATED: bool, T: StringSearchImpl> StringSearchCreator<NEGATED, T> {
    pub fn try_create_func(
        display_name: &str,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        for arg in args {
            // variant data can compare with string
            if arg.data_type_id().is_variant() {
                continue;
            }
            assert_string(arg)?;
        }

        let f: StringSearchFn = match NEGATED {
            true => |x| !x,
            false => |x| x,
        };

        let func = Arc::new(ComparisonStringImpl::<T>::new(f));
        ComparisonFunction::try_create_func(display_name, func)
    }

    pub fn desc(negative_name: &str) -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function(negative_name)
                .num_arguments(2),
        )
    }
}

pub trait ComparisonImpl: Sync + Send + Clone + 'static {
    type BooleanSimd: BooleanSimdImpl;

    fn eval_simd<T>(l: T::Simd, r: T::Simd) -> u8
    where
        T: PrimitiveType + comparison::Simd8,
        T::Simd: comparison::Simd8PartialEq + comparison::Simd8PartialOrd;

    fn eval_primitive<L, R, M>(
        _l: L::RefType<'_>,
        _r: R::RefType<'_>,
        _ctx: &mut EvalContext,
    ) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType;

    fn eval_binary(_l: &[u8], _r: &[u8], _ctx: &mut EvalContext) -> bool;

    fn eval_variant(_l: &VariantValue, _r: &VariantValue, _ctx: &mut EvalContext) -> bool;
}

pub trait ComparisonExpression: Sync + Send {
    fn eval(&self, l: &ColumnWithField, r: &ColumnWithField) -> Result<BooleanColumn>;
}

pub struct ComparisonScalarImpl<L: Scalar, R: Scalar, F> {
    least_supertype: DataTypeImpl,
    need_cast: bool,
    func: F,
    _phantom: PhantomData<(L, R)>,
}

impl<L: Scalar, R: Scalar, F> ComparisonScalarImpl<L, R, F>
where F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> bool
{
    pub fn new(least_supertype: DataTypeImpl, need_cast: bool, func: F) -> Self {
        Self {
            least_supertype,
            need_cast,
            func,
            _phantom: PhantomData,
        }
    }
}

impl<L, R, F> ComparisonExpression for ComparisonScalarImpl<L, R, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> bool + Send + Sync + Clone,
{
    fn eval(&self, l: &ColumnWithField, r: &ColumnWithField) -> Result<BooleanColumn> {
        let func_ctx = FunctionContext::default();
        let lhs = if self.need_cast && l.data_type() != &self.least_supertype {
            cast_column_field(
                l,
                l.data_type(),
                &wrap_nullable(&self.least_supertype),
                &func_ctx,
            )?
        } else {
            l.column().clone()
        };
        let rhs = if self.need_cast && r.data_type() != &self.least_supertype {
            cast_column_field(
                r,
                r.data_type(),
                &wrap_nullable(&self.least_supertype),
                &func_ctx,
            )?
        } else {
            r.column().clone()
        };

        let (_, l_bitmap) = lhs.validity();
        let (_, r_bitmap) = rhs.validity();
        let bitmap = combine_validities(l_bitmap, r_bitmap);
        let lhs = Series::remove_nullable(&lhs);
        let rhs = Series::remove_nullable(&rhs);
        let column = scalar_binary_op(&lhs, &rhs, self.func.clone(), &mut EvalContext::default())?;
        match bitmap {
            Some(bitmap) => {
                let bitmap = combine_validities(Some(column.values()), Some(&bitmap)).unwrap();
                Ok(BooleanColumn::from_arrow_data(bitmap))
            }
            None => Ok(column),
        }
    }
}

pub struct ComparisonPrimitiveImpl<T: PrimitiveType, F> {
    least_supertype: DataTypeImpl,
    need_cast: bool,
    func: F,
    _phantom: PhantomData<T>,
}

impl<T, F> ComparisonPrimitiveImpl<T, F>
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    pub fn new(least_supertype: DataTypeImpl, need_cast: bool, func: F) -> Self {
        Self {
            least_supertype,
            need_cast,
            func,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> ComparisonExpression for ComparisonPrimitiveImpl<T, F>
where
    T: PrimitiveType + comparison::Simd8 + Send + Sync + Clone,
    F: Fn(T::Simd, T::Simd) -> u8 + Send + Sync + Clone,
{
    fn eval(&self, l: &ColumnWithField, r: &ColumnWithField) -> Result<BooleanColumn> {
        let func_ctx = FunctionContext::default();
        let lhs = if self.need_cast && l.data_type() != &self.least_supertype {
            cast_column_field(
                l,
                l.data_type(),
                &wrap_nullable(&self.least_supertype),
                &func_ctx,
            )?
        } else {
            l.column().clone()
        };

        let rhs = if self.need_cast && r.data_type() != &self.least_supertype {
            cast_column_field(
                r,
                r.data_type(),
                &wrap_nullable(&self.least_supertype),
                &func_ctx,
            )?
        } else {
            r.column().clone()
        };

        let (_, l_bitmap) = lhs.validity();
        let (_, r_bitmap) = rhs.validity();
        let bitmap = combine_validities(l_bitmap, r_bitmap);
        let lhs = Series::remove_nullable(&lhs);
        let rhs = Series::remove_nullable(&rhs);
        let column = primitive_simd_op_boolean::<T, F>(&lhs, &rhs, self.func.clone())?;
        match bitmap {
            Some(bitmap) => {
                let bitmap = combine_validities(Some(column.values()), Some(&bitmap)).unwrap();
                Ok(BooleanColumn::from_arrow_data(bitmap))
            }
            None => Ok(column),
        }
    }
}

pub struct ComparisonBooleanImpl<F> {
    _phantom: PhantomData<F>,
}

impl<F: BooleanSimdImpl> ComparisonBooleanImpl<F> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<F: BooleanSimdImpl> ComparisonExpression for ComparisonBooleanImpl<F> {
    fn eval(&self, l: &ColumnWithField, r: &ColumnWithField) -> Result<BooleanColumn> {
        let func_ctx = FunctionContext::default();
        let lhs = if l.data_type().data_type_id() != TypeID::Boolean {
            cast_column_field(
                l,
                l.data_type(),
                &wrap_nullable(&DataTypeImpl::Boolean(BooleanType::default())),
                &func_ctx,
            )?
        } else {
            l.column().clone()
        };
        let rhs = if r.data_type().data_type_id() != TypeID::Boolean {
            cast_column_field(
                r,
                r.data_type(),
                &wrap_nullable(&DataTypeImpl::Boolean(BooleanType::default())),
                &func_ctx,
            )?
        } else {
            r.column().clone()
        };

        let res = match (lhs.is_const(), rhs.is_const()) {
            (false, false) => {
                let (_, l_bitmap) = lhs.validity();
                let (_, r_bitmap) = rhs.validity();
                let bitmap = combine_validities(l_bitmap, r_bitmap);
                let lhs = Series::remove_nullable(&lhs);
                let rhs = Series::remove_nullable(&rhs);
                let lhs: &BooleanColumn = Series::check_get(&lhs)?;
                let rhs: &BooleanColumn = Series::check_get(&rhs)?;
                let column = F::vector_vector(lhs, rhs);
                match bitmap {
                    Some(bitmap) => {
                        let bitmap =
                            combine_validities(Some(column.values()), Some(&bitmap)).unwrap();
                        BooleanColumn::from_arrow_data(bitmap)
                    }
                    None => column,
                }
            }
            (false, true) => {
                let (_, l_bitmap) = lhs.validity();
                let lhs = Series::remove_nullable(&lhs);
                let lhs: &BooleanColumn = Series::check_get(&lhs)?;
                let r = rhs.get_bool(0);
                if r.is_err() {
                    let bitmap = new_mutable_bitmap(lhs.len(), false);
                    return Ok(BooleanColumn::from_arrow_data(bitmap.into()));
                };
                let column = F::vector_const(lhs, r.unwrap());
                match l_bitmap {
                    Some(bitmap) => {
                        let bitmap =
                            combine_validities(Some(column.values()), Some(bitmap)).unwrap();
                        BooleanColumn::from_arrow_data(bitmap)
                    }
                    None => column,
                }
            }
            (true, false) => {
                let (_, r_bitmap) = rhs.validity();
                let rhs = Series::remove_nullable(&rhs);
                let rhs: &BooleanColumn = Series::check_get(&rhs)?;
                let l = lhs.get_bool(0);
                if l.is_err() {
                    let bitmap = new_mutable_bitmap(rhs.len(), false);
                    return Ok(BooleanColumn::from_arrow_data(bitmap.into()));
                }
                let column = F::const_vector(l.unwrap(), rhs);
                match r_bitmap {
                    Some(bitmap) => {
                        let bitmap =
                            combine_validities(Some(column.values()), Some(bitmap)).unwrap();
                        BooleanColumn::from_arrow_data(bitmap)
                    }
                    None => column,
                }
            }
            (true, true) => unreachable!(),
        };
        Ok(res)
    }
}

type StringSearchFn = fn(bool) -> bool;

pub struct ComparisonStringImpl<T> {
    op: StringSearchFn,
    _phantom: PhantomData<T>,
}

impl<T: StringSearchImpl> ComparisonStringImpl<T> {
    pub fn new(op: StringSearchFn) -> Self {
        Self {
            op,
            _phantom: PhantomData,
        }
    }
}

impl<T: StringSearchImpl> ComparisonExpression for ComparisonStringImpl<T> {
    fn eval(&self, l: &ColumnWithField, r: &ColumnWithField) -> Result<BooleanColumn> {
        let func_ctx = FunctionContext::default();
        let lhs = if !l.data_type().data_type_id().is_string() {
            cast_column_field(
                l,
                l.data_type(),
                &DataTypeImpl::String(StringType::default()),
                &func_ctx,
            )?
        } else {
            l.column().clone()
        };
        let rhs = if !r.data_type().data_type_id().is_string() {
            cast_column_field(
                r,
                r.data_type(),
                &DataTypeImpl::String(StringType::default()),
                &func_ctx,
            )?
        } else {
            r.column().clone()
        };
        let res = match rhs.is_const() {
            true => {
                let lhs: &StringColumn = Series::check_get(&lhs)?;
                let r = rhs.get_string(0)?;
                T::vector_const(lhs, &r, self.op)
            }
            false => {
                let full_lhs = lhs.convert_full_column();
                let lhs: &StringColumn = match Series::check_get(&lhs) {
                    Ok(lhs) => lhs,
                    Err(_) => Series::check_get(&full_lhs)?,
                };
                let full_rhs = rhs.convert_full_column();
                let rhs: &StringColumn = match Series::check_get(&rhs) {
                    Ok(lhs) => lhs,
                    Err(_) => Series::check_get(&full_rhs)?,
                };
                T::vector_vector(lhs, rhs, self.op)
            }
        };
        Ok(res)
    }
}
