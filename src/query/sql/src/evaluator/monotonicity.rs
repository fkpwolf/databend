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

use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionContext;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::Monotonicity;
use common_planner::Expression;
use common_planner::ExpressionVisitor;
use common_planner::Recursion;

// ExpressionMonotonicityVisitor visit the expression tree to calculate monotonicity.
// For example, a function of Add(Neg(number), 5) for number < -100 will have a tree like this:
//
// .                   MonotonicityNode::Function -- 'Add'
//                      (mono: is_positive=true, Range{105, MAX})
//                         /                          \
//                        /                            \
//      MonotonicityNode::Function -- f(x)=-x         Monotonicity::Constant -- 5
//    (mono: is_positive=true, range{100, MAX})
//                     /
//                    /
//     MonotonicityNode::Function -- f(x)=x
//         (range{MIN, -100})
//
// The structure of the tree is basically the structure of the expression.
// Simple depth first search visit the expression tree and gete monotonicity from
// every function. Each function is responsible to implement its own monotonicity
// function.
#[derive(Clone)]
pub struct ExpressionMonotonicityVisitor {
    input_schema: DataSchemaRef,
    // HashMap<column_name, (variable_left, variable_right)>
    // variable_left: the variable range left.
    // variable_right: the variable range right.
    variables: HashMap<String, (Option<ColumnWithField>, Option<ColumnWithField>)>,
    stack: Vec<(DataTypeImpl, Monotonicity)>,

    single_point: bool,
}

impl ExpressionMonotonicityVisitor {
    fn create(
        input_schema: DataSchemaRef,
        variables: HashMap<String, (Option<ColumnWithField>, Option<ColumnWithField>)>,
        single_point: bool,
    ) -> Self {
        Self {
            input_schema,
            variables,
            stack: vec![],
            single_point,
        }
    }

    pub fn finalize(mut self) -> Result<Monotonicity> {
        match self.stack.len() {
            1 => {
                let (_, monotonic) = self.stack.remove(0);
                Ok(monotonic)
            }
            _ => Err(ErrorCode::Internal(
                "Stack has too many elements in ExpressionMonotonicityVisitor::finalize",
            )),
        }
    }

    fn try_calculate_boundary(
        func: &dyn Function,
        result_type: &DataTypeImpl,
        args: Vec<Option<ColumnWithField>>,
    ) -> Result<Option<ColumnWithField>> {
        if args.iter().any(|col| col.is_none()) {
            Ok(None)
        } else {
            let input_columns = args
                .into_iter()
                .map(|col_opt| col_opt.unwrap())
                .collect::<Vec<_>>();
            // TODO(veeupup): whether we need to pass function context here?
            let col = func.eval(FunctionContext::default(), &input_columns, 1)?;
            let data_field = DataField::new("dummy", result_type.clone());
            let data_column_field = ColumnWithField::new(col, data_field);
            Ok(Some(data_column_field))
        }
    }

    fn visit_function(mut self, op: &str, args_size: usize) -> Result<Self> {
        let mut left_vec = Vec::with_capacity(args_size);
        let mut right_vec = Vec::with_capacity(args_size);
        let mut arg_types = Vec::with_capacity(args_size);
        let mut monotonicity_vec = Vec::with_capacity(args_size);

        for index in 0..args_size {
            match self.stack.pop() {
                None => {
                    return Err(ErrorCode::Internal(format!(
                        "Expected {} arguments, actual {}.",
                        args_size, index
                    )));
                }
                Some((arg_type, monotonic)) => {
                    left_vec.push(monotonic.left.clone());
                    right_vec.push(monotonic.right.clone());
                    arg_types.push(arg_type);
                    monotonicity_vec.push(monotonic);
                }
            }
        }

        let instance = FunctionFactory::instance();

        let arg_types: Vec<&DataTypeImpl> = arg_types.iter().collect();
        let func = instance.get(op, &arg_types)?;

        let return_type = func.return_type();
        let mut monotonic = match self.single_point {
            false => func.get_monotonicity(monotonicity_vec.as_ref())?,
            true => {
                let features = instance.get_features(op)?;
                if features.is_deterministic {
                    Monotonicity::create_constant()
                } else {
                    Monotonicity::default()
                }
            }
        };

        // Neither a monotonic expression nor constant, interrupt the traversal and return an error directly.
        if !monotonic.is_monotonic && !monotonic.is_constant {
            return Err(ErrorCode::UnknownException(format!(
                "Function '{}' is not monotonic in the variables range",
                op
            )));
        }

        monotonic.left = Self::try_calculate_boundary(func.as_ref(), &return_type, left_vec)?;
        monotonic.right = Self::try_calculate_boundary(func.as_ref(), &return_type, right_vec)?;

        self.stack.push((return_type, monotonic));
        Ok(self)
    }

    /// Check whether the expression is monotonic or not. The left should be <= right.
    /// Return the monotonicity information, together with column name if any.
    pub fn check_expression(
        schema: DataSchemaRef,
        expr: &Expression,
        variables: HashMap<String, (Option<ColumnWithField>, Option<ColumnWithField>)>,
        single_point: bool,
    ) -> Monotonicity {
        let visitor = Self::create(schema, variables, single_point);
        visitor.visit(expr).map_or(Monotonicity::default(), |v| {
            v.finalize().unwrap_or_else(|_| Monotonicity::default())
        })
    }
}

impl ExpressionVisitor for ExpressionMonotonicityVisitor {
    fn pre_visit(self, _expr: &Expression) -> Result<Recursion<Self>> {
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::IndexedVariable { name, .. } => {
                let (left, right) = self.variables.get(name).ok_or_else(|| {
                    ErrorCode::BadArguments(format!("Cannot find the column: '{:?}'", name))
                })?;

                let field = self.input_schema.field_with_name(name)?;
                let return_type = field.data_type();

                let monotonic = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: false,
                    left: left.clone(),
                    right: right.clone(),
                };

                self.stack.push((return_type.clone(), monotonic));
                Ok(self)
            }
            Expression::Constant { value, data_type } => {
                let name = value.to_string();
                let data_field = DataField::new(&name, data_type.clone());

                let col = data_type.create_constant_column(value, 1)?;
                let data_column_field = ColumnWithField::new(col, data_field);
                let monotonic = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: true,
                    left: Some(data_column_field.clone()),
                    right: Some(data_column_field),
                };

                self.stack.push((data_type.clone(), monotonic));
                Ok(self)
            }
            Expression::Function { name, args, .. } => self.visit_function(name, args.len()),
            _ => Err(ErrorCode::UnknownException("Unable to get monotonicity")),
        }
    }
}
