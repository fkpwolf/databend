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

mod expression_parser;
mod format;
mod metadata;
#[allow(clippy::module_inception)]
mod planner;
mod planner_context;
mod semantic;

pub mod binder;
pub mod optimizer;
pub mod plans;

pub use binder::BindContext;
pub use binder::Binder;
pub use binder::ColumnBinding;
pub use binder::ScalarBinder;
pub use binder::SelectBuilder;
pub use binder::Visibility;
pub use expression_parser::ExpressionParser;
pub use metadata::*;
pub use planner::Planner;
pub use planner_context::PlannerContext;
pub use plans::ScalarExpr;
pub use semantic::normalize_identifier;
pub use semantic::validate_function_arg;
pub use semantic::IdentifierNormalizer;
pub use semantic::NameResolutionContext;
