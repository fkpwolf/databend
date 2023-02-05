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

use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::tokio;
use common_meta_kvapi::kvapi;
use databend_meta::init_meta_ut;

use crate::tests::service::MetaSrvBuilder;

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_metasrv_kv_api() -> anyhow::Result<()> {
    let builder = MetaSrvBuilder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    kvapi::TestSuite {}.test_all(builder).await
}
