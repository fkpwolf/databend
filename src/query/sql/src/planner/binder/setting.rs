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

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::UnSetSource;
use common_ast::ast::UnSetStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use super::BindContext;
use super::Binder;
use crate::planner::semantic::TypeChecker;
use crate::plans::CastExpr;
use crate::plans::Plan;
use crate::plans::ScalarExpr;
use crate::plans::SettingPlan;
use crate::plans::UnSettingPlan;
use crate::plans::VarValue;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_set_variable(
        &mut self,
        bind_context: &BindContext,
        is_global: bool,
        variable: &Identifier<'a>,
        value: &Expr<'a>,
    ) -> Result<Plan> {
        let mut type_checker = TypeChecker::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let variable = variable.name.clone();

        let (scalar, data_type) = *type_checker.resolve(value, None).await?;
        let scalar = ScalarExpr::CastExpr(CastExpr {
            is_try: false,
            argument: Box::new(scalar),
            from_type: Box::new(data_type),
            target_type: Box::new(DataType::String),
        });
        let expr = scalar.as_expr_with_col_index()?;

        let (new_expr, _) = ConstantFolder::fold(
            &expr,
            self.ctx.try_get_function_context()?,
            &BUILTIN_FUNCTIONS,
        );
        match new_expr {
            common_expression::Expr::Constant { scalar, .. } => {
                let value = String::from_utf8(scalar.into_string().unwrap())?;
                let vars = vec![VarValue {
                    is_global,
                    variable,
                    value,
                }];
                Ok(Plan::SetVariable(Box::new(SettingPlan { vars })))
            }
            _ => Err(ErrorCode::SemanticError("value must be constant value")),
        }
    }

    pub(in crate::planner::binder) async fn bind_unset_variable(
        &mut self,
        _bind_context: &BindContext,
        stmt: &UnSetStmt<'_>,
    ) -> Result<Plan> {
        match stmt.clone().source {
            UnSetSource::Var { variable } => {
                let variable = variable.name;
                let vars = vec![variable];
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
            UnSetSource::Vars { variables } => {
                let mut vars: Vec<String> = vec![];
                for var in variables {
                    vars.push(var.name.clone());
                }
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
        }
    }
}
