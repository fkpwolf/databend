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
use common_datavalues::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowRolesPlan {}

impl ShowRolesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("inherited_roles", u64::to_data_type()),
            DataField::new("is_current", bool::to_data_type()),
            DataField::new("is_default", bool::to_data_type()),
        ])
    }
}
