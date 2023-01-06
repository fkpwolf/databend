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

mod cache;
mod cache_metrics;
mod memory_cache;
mod memory_cache_reader;

pub use cache::CacheManager;
pub use cache::LoadParams;
pub use cache::Loader;
pub use cache_metrics::metrics_reset;
pub use memory_cache::ItemCache;
pub use memory_cache::LabeledItemCache;
pub use memory_cache::SegmentInfoCache;
pub use memory_cache::TableSnapshotCache;
pub use memory_cache::TableSnapshotStatisticCache;
pub use memory_cache_reader::MemoryCacheReader;
