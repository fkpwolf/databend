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

mod catalog;
mod config;
mod context;

mod sessions;
pub(crate) mod tls_constants;

pub use catalog::create_catalog;
pub use config::ConfigBuilder;
pub use context::create_query_context;
pub use context::create_query_context_with_cluster;
pub use context::create_query_context_with_config;
pub use context::create_query_context_with_session;
pub use context::ClusterDescriptor;
pub use sessions::TestGlobalServices;
pub use sessions::TestGuard;
