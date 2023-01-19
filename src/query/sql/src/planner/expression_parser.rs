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

use std::sync::Arc;

use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_base::base::tokio::runtime::Handle;
use common_base::base::tokio::task::block_in_place;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_settings::Settings;
use parking_lot::RwLock;

use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::Metadata;
use crate::Visibility;

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    unwrap_tuple: bool,
    sql: &str,
) -> Result<Vec<Expr>> {
    let settings = Settings::default_settings("", GlobalConfig::instance())?;
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
    );

    let columns = metadata.read().columns_by_table_index(table_index);
    let table = metadata.read().table(table_index).clone();
    for (index, column) in columns.iter().enumerate() {
        let column_binding = match column {
            ColumnEntry::BaseTableColumn {
                column_name,
                data_type,
                path_indices,
                ..
            } => ColumnBinding {
                database_name: Some("default".to_string()),
                table_name: Some(table.name().to_string()),
                column_name: column_name.clone(),
                index,
                data_type: Box::new(data_type.into()),
                visibility: if path_indices.is_some() {
                    Visibility::InVisible
                } else {
                    Visibility::Visible
                },
            },

            _ => {
                return Err(ErrorCode::Internal("Invalid column entry"));
            }
        };

        bind_context.add_column_binding(column_binding);
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker =
        TypeChecker::new(&bind_context, ctx, &name_resolution_ctx, metadata, &[]);

    let sql_dialect = Dialect::MySQL;
    let tokens = tokenize_sql(sql)?;
    let backtrace = Backtrace::new();
    let tokens = if unwrap_tuple {
        &tokens[1..tokens.len() - 1]
    } else {
        &tokens
    };
    let ast_exprs = parse_comma_separated_exprs(tokens, sql_dialect, &backtrace)?;
    let exprs = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) =
                *block_in_place(|| Handle::current().block_on(type_checker.resolve(ast, None)))?;
            let expr = scalar.as_expr_with_col_index()?;
            Ok(expr)
        })
        .collect::<Result<_>>()?;

    Ok(exprs)
}

pub fn parse_to_remote_string_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    unwrap_tuple: bool,
    sql: &str,
) -> Result<Vec<RemoteExpr<String>>> {
    let schema = table_meta.schema();
    let exprs = parse_exprs(ctx, table_meta, unwrap_tuple, sql)?;
    let exprs = exprs
        .iter()
        .map(|expr| {
            expr.project_column_ref(|index| schema.field(*index).name().to_string())
                .as_remote_expr()
        })
        .collect();

    Ok(exprs)
}
