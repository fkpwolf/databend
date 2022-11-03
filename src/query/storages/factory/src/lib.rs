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

pub use common_storages_fuse as fuse;
pub use common_storages_index as index;
pub mod result;
mod storage_factory;

pub use common_catalog::table::NavigationPoint;
pub use common_catalog::table::Table;
pub use common_catalog::table::TableStatistics;
pub use common_storages_preludes::information_schema;
pub use common_storages_preludes::memory;
pub use common_storages_preludes::null;
pub use common_storages_preludes::random;
pub use common_storages_preludes::view;
pub use storage_factory::StorageCreator;
pub use storage_factory::StorageDescription;
pub use storage_factory::StorageFactory;
