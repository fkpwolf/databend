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
use std::ops::BitAnd;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::date_helper::TzLUT;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::type_check::try_unify_signature;
use crate::types::nullable::NullableColumn;
use crate::types::*;
use crate::utils::arrow::constant_bitmap;
use crate::values::Value;
use crate::values::ValueRef;
use crate::Column;
use crate::ColumnIndex;
use crate::Expr;
use crate::FunctionDomain;
use crate::Scalar;

#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub name: String,
    pub args_type: Vec<DataType>,
    pub return_type: DataType,
    pub property: FunctionProperty,
}

pub type AutoCastRules<'a> = &'a [(DataType, DataType)];

#[derive(Clone, Copy, Default)]
pub struct FunctionContext {
    pub tz: TzLUT,
}

#[derive(Clone)]
pub struct EvalContext<'a> {
    pub generics: &'a GenericMap,
    pub num_rows: usize,
    pub tz: TzLUT,

    /// Validity bitmap of outer nullable column. This is an optimization
    /// to avoid recording errors on the NULL value which has a corresponding
    /// default value in nullable's inner column.
    pub validity: Option<Bitmap>,
    pub errors: Option<(MutableBitmap, String)>,
}

impl<'a> EvalContext<'a> {
    #[inline]
    pub fn set_error(&mut self, row: usize, error_msg: impl AsRef<str>) {
        // If the row is NULL, we don't need to set error.
        if self
            .validity
            .as_ref()
            .map(|b| !b.get_bit(row))
            .unwrap_or(false)
        {
            return;
        }

        match self.errors.as_mut() {
            Some((valids, _)) => {
                valids.set(row, false);
            }
            None => {
                let mut valids = constant_bitmap(true, self.num_rows.max(1));
                valids.set(row, false);
                self.errors = Some((valids, error_msg.as_ref().to_string()));
            }
        }
    }

    pub fn render_error(&self, span: Span, args: &[Value<AnyType>], func_name: &str) -> Result<()> {
        match &self.errors {
            Some((valids, error)) => {
                let first_error_row = valids
                    .iter()
                    .enumerate()
                    .filter(|(_, valid)| !valid)
                    .take(1)
                    .next()
                    .unwrap()
                    .0;
                let args = args
                    .iter()
                    .map(|arg| {
                        let arg_ref = arg.as_ref();
                        arg_ref.index(first_error_row).unwrap().to_string()
                    })
                    .join(", ");

                Err(ErrorCode::Internal(format!(
                    "{error} while evaluating function `{func_name}({args})`"
                ))
                .set_span(span))
            }
            None => Ok(()),
        }
    }
}

/// `FunctionID` is a unique identifier for a function. It's used to construct
/// the exactly same function from the remote execution nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionID {
    Builtin {
        name: String,
        id: usize,
    },
    Factory {
        name: String,
        id: usize,
        params: Vec<usize>,
        args_type: Vec<DataType>,
    },
}

pub struct Function {
    pub signature: FunctionSignature,
    #[allow(clippy::type_complexity)]
    pub calc_domain: Box<dyn Fn(&[Domain]) -> FunctionDomain<AnyType> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pub eval: Box<dyn Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync>,
}

impl Function {
    pub fn wrap_nullable(self) -> Self {
        Self {
            signature: FunctionSignature {
                name: self.signature.name.clone(),
                args_type: self
                    .signature
                    .args_type
                    .iter()
                    .map(|ty| ty.wrap_nullable())
                    .collect(),
                return_type: self.signature.return_type.wrap_nullable(),
                property: self.signature.property.clone(),
            },
            calc_domain: Box::new(|_| FunctionDomain::Full),
            eval: Box::new(wrap_nullable(self.eval)),
        }
    }
}

/// A function to build function depending on the const parameters and the type of arguments (before coercion).
///
/// The first argument is the const parameters and the second argument is the types of arguments.
pub type FunctionFactory =
    Box<dyn Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + Send + Sync + 'static>;

#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<String, Vec<(Arc<Function>, usize)>>,
    pub factories: HashMap<String, Vec<(FunctionFactory, usize)>>,

    /// Aliases map from alias function name to original function name.
    pub aliases: HashMap<String, String>,

    /// Default cast rules for all functions.
    pub default_cast_rules: Vec<(DataType, DataType)>,
    /// Cast rules for specific functions, excluding the default cast rules.
    pub additional_cast_rules: HashMap<String, Vec<(DataType, DataType)>>,
    /// The auto rules that should use TRY_CAST instead of CAST.
    pub auto_try_cast_rules: Vec<(DataType, DataType)>,
}

impl FunctionRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn registered_names(&self) -> Vec<String> {
        self.funcs
            .keys()
            .chain(self.factories.keys())
            .chain(self.aliases.keys())
            .unique()
            .cloned()
            .collect()
    }

    pub fn contains(&self, func_name: &str) -> bool {
        self.funcs.contains_key(func_name)
            || self.factories.contains_key(func_name)
            || self.aliases.contains_key(func_name)
    }

    pub fn get(&self, id: &FunctionID) -> Option<Arc<Function>> {
        match id {
            FunctionID::Builtin { name, id } => self
                .funcs
                .get(name.as_str())?
                .iter()
                .find(|(_, func_id)| func_id == id)
                .map(|(func, _)| func.clone()),
            FunctionID::Factory {
                name,
                id,
                params,
                args_type,
            } => {
                let factory = self
                    .factories
                    .get(name.as_str())?
                    .iter()
                    .find(|(_, func_id)| func_id == id)
                    .map(|(func, _)| func)?;
                factory(params, args_type)
            }
        }
    }

    pub fn search_candidates<Index: ColumnIndex>(
        &self,
        name: &str,
        params: &[usize],
        args: &[Expr<Index>],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        let name = name.to_lowercase();

        let mut candidates = Vec::new();

        if let Some(funcs) = self.funcs.get(&name) {
            candidates.extend(funcs.iter().filter_map(|(func, id)| {
                if func.signature.name == name && func.signature.args_type.len() == args.len() {
                    Some((
                        FunctionID::Builtin {
                            name: name.to_string(),
                            id: *id,
                        },
                        func.clone(),
                    ))
                } else {
                    None
                }
            }));
        }

        if let Some(factories) = self.factories.get(&name) {
            let args_type = args
                .iter()
                .map(Expr::data_type)
                .cloned()
                .collect::<Vec<_>>();
            candidates.extend(factories.iter().filter_map(|(factory, id)| {
                factory(params, &args_type).map(|func| {
                    (
                        FunctionID::Factory {
                            name: name.to_string(),
                            id: *id,
                            params: params.to_vec(),
                            args_type: args_type.clone(),
                        },
                        func,
                    )
                })
            }));
        }

        candidates.sort_by_key(|(id, _)| id.id());

        candidates
    }

    pub fn get_auto_cast_rules(&self, func_name: &str) -> &[(DataType, DataType)] {
        self.additional_cast_rules
            .get(func_name)
            .unwrap_or(&self.default_cast_rules)
    }

    pub fn is_auto_try_cast_rule(&self, arg_type: &DataType, sig_type: &DataType) -> bool {
        self.auto_try_cast_rules
            .iter()
            .any(|(src_ty, dest_ty)| arg_type == src_ty && sig_type == dest_ty)
    }

    pub fn register_function_factory(
        &mut self,
        name: &str,
        factory: impl Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + 'static + Send + Sync,
    ) {
        let id = self.next_function_id(name);
        self.factories
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push((Box::new(factory), id));
    }

    pub fn register_aliases(&mut self, fn_name: &str, aliases: &[&str]) {
        for alias in aliases {
            self.aliases.insert(alias.to_string(), fn_name.to_string());
        }
    }

    pub fn register_default_cast_rules(
        &mut self,
        default_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.default_cast_rules
            .extend(default_cast_rules.into_iter());
    }

    pub fn register_additional_cast_rules(
        &mut self,
        fn_name: &str,
        additional_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.additional_cast_rules
            .entry(fn_name.to_string())
            .or_insert_with(Vec::new)
            .extend(additional_cast_rules.into_iter());
    }

    pub fn register_auto_try_cast_rules(
        &mut self,
        auto_try_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.auto_try_cast_rules.extend(auto_try_cast_rules);
    }

    pub fn next_function_id(&self, name: &str) -> usize {
        self.funcs.get(name).map(|funcs| funcs.len()).unwrap_or(0)
            + self.factories.get(name).map(|f| f.len()).unwrap_or(0)
    }

    pub fn check_ambiguity(&self) {
        for (name, funcs) in &self.funcs {
            let auto_cast_rules = self.get_auto_cast_rules(name);
            for (former, former_id) in funcs {
                for latter in funcs
                    .iter()
                    .filter(|(_, id)| id > former_id)
                    .map(|(func, _)| func.clone())
                    .chain(
                        self.factories
                            .get(name)
                            .map(Vec::as_slice)
                            .unwrap_or(&[])
                            .iter()
                            .filter(|(_, id)| id > former_id)
                            .filter_map(|(factory, _)| factory(&[], &former.signature.args_type)),
                    )
                {
                    if former.signature.args_type.len() == latter.signature.args_type.len() {
                        if let Ok(subst) = try_unify_signature(
                            latter.signature.args_type.iter(),
                            former.signature.args_type.iter(),
                            auto_cast_rules,
                        ) {
                            if subst.apply(&former.signature.return_type).is_ok()
                                && former
                                    .signature
                                    .args_type
                                    .iter()
                                    .all(|sig_ty| subst.apply(sig_ty).is_ok())
                            {
                                panic!(
                                    "Ambiguous signatures for function:\n- {}\n- {}\n\
                                        Suggestion: swap the order of the overloads.",
                                    former.signature, latter.signature
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

impl FunctionID {
    pub fn id(&self) -> usize {
        match self {
            FunctionID::Builtin { id, .. } => *id,
            FunctionID::Factory { id, .. } => *id,
        }
    }
}

pub fn wrap_nullable<F>(f: F) -> impl Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType>
where F: Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> {
    move |args, ctx| {
        type T = NullableType<AnyType>;
        type Result = AnyType;

        let mut bitmap: Option<MutableBitmap> = None;
        let mut nonull_args: Vec<ValueRef<Result>> = Vec::with_capacity(args.len());

        let mut len = 1;
        for arg in args {
            let arg = arg.try_downcast::<T>().unwrap();
            match arg {
                ValueRef::Scalar(None) => return Value::Scalar(Scalar::Null),
                ValueRef::Scalar(Some(s)) => {
                    nonull_args.push(ValueRef::Scalar(s.clone()));
                }
                ValueRef::Column(v) => {
                    len = v.len();
                    nonull_args.push(ValueRef::Column(v.column.clone()));
                    bitmap = match bitmap {
                        Some(m) => Some(m.bitand(&v.validity)),
                        None => Some(v.validity.clone().make_mut()),
                    };
                }
            }
        }
        let results = f(&nonull_args, ctx);
        let bitmap = bitmap.unwrap_or_else(|| constant_bitmap(true, len));
        match results {
            Value::Scalar(s) => {
                if bitmap.get(0) {
                    Value::Scalar(s)
                } else {
                    Value::Scalar(Scalar::Null)
                }
            }
            Value::Column(column) => {
                let result = match column {
                    Column::Nullable(box nullable_column) => {
                        let validity = bitmap.into();
                        let validity =
                            common_arrow::arrow::bitmap::and(&nullable_column.validity, &validity);
                        Column::Nullable(Box::new(NullableColumn {
                            column: nullable_column.column,
                            validity,
                        }))
                    }
                    _ => Column::Nullable(Box::new(NullableColumn {
                        column,
                        validity: bitmap.into(),
                    })),
                };
                Value::Column(result)
            }
        }
    }
}

pub fn error_to_null<I1: ArgType, O: ArgType>(
    func: impl for<'a> Fn(ValueRef<'a, I1>, &mut EvalContext) -> Value<O> + Copy + Send + Sync,
) -> impl for<'a> Fn(ValueRef<'a, I1>, &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync
{
    move |val, ctx| {
        let output = func(val, ctx);
        if let Some((validity, _)) = ctx.errors.take() {
            match output {
                Value::Scalar(_) => Value::Scalar(None),
                Value::Column(column) => Value::Column(NullableColumn {
                    column,
                    validity: validity.into(),
                }),
            }
        } else {
            match output {
                Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
                Value::Column(column) => Value::Column(NullableColumn {
                    column,
                    validity: constant_bitmap(true, ctx.num_rows).into(),
                }),
            }
        }
    }
}
