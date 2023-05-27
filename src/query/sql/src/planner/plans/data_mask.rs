// Copyright 2021 Datafuse Labs
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

use chrono::Utc;
use common_ast::ast::DataMaskPolicy;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_meta_app::data_mask::CreateDatamaskReq;
use common_meta_app::data_mask::DatamaskNameIdent;
use common_meta_app::data_mask::DropDatamaskReq;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateDatamaskPolicyPlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl CreateDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<CreateDatamaskPolicyPlan> for CreateDatamaskReq {
    fn from(p: CreateDatamaskPolicyPlan) -> Self {
        CreateDatamaskReq {
            if_not_exists: p.if_not_exists,
            name: DatamaskNameIdent {
                tenant: p.tenant.clone(),
                name: p.name.clone(),
            },
            args: p
                .policy
                .args
                .iter()
                .map(|arg| (arg.arg_name.to_string(), arg.arg_type.to_string()))
                .collect(),
            return_type: p.policy.return_type.to_string(),
            body: p.policy.body.to_string(),
            comment: p.policy.comment,
            create_on: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropDatamaskPolicyPlan {
    pub if_exists: bool,
    pub tenant: String,
    pub name: String,
}

impl DropDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<DropDatamaskPolicyPlan> for DropDatamaskReq {
    fn from(p: DropDatamaskPolicyPlan) -> Self {
        DropDatamaskReq {
            if_exists: p.if_exists,
            name: DatamaskNameIdent {
                tenant: p.tenant.clone(),
                name: p.name,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescDatamaskPolicyPlan {
    pub name: String,
}

impl DescDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("Name", DataType::String),
            DataField::new("Create On", DataType::String),
            DataField::new("Signature", DataType::String),
            DataField::new("Return Type", DataType::String),
            DataField::new("Body", DataType::String),
            DataField::new("Comment", DataType::String),
        ]))
    }
}
