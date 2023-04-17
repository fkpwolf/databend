// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;
use common_storages_share::get_share_spec_location;

use crate::accessor::SharingAccessor;
use crate::models;
use crate::models::ShareSpec;

// Methods for access share spec by tenant.
impl SharingAccessor {
    #[async_backtrace::framed]
    pub async fn get_share_spec(tenant: &String) -> Result<Vec<ShareSpec>> {
        let sharing_accessor = Self::instance();
        let path = get_share_spec_location(&sharing_accessor.config.tenant);
        let data = sharing_accessor.op.read(&path).await?;
        let share_specs: models::SharingConfig = serde_json::from_slice(data.as_slice())?;
        let mut share_spec_vec = vec![];

        for (_, share_spec) in share_specs.share_specs {
            if share_spec.tenants.contains(tenant) {
                share_spec_vec.push(share_spec);
            }
        }
        Ok(share_spec_vec)
    }
}
