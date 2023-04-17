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

use std::time::Duration;

use common_base::base::tokio;
use common_meta_client::MetaGrpcClient;
use databend_meta::init_meta_ut;
use pretty_assertions::assert_eq;
use regex::Regex;

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_get_client_info() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Get client ip

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        Duration::from_secs(10),
        None,
    )?;

    let resp = client.get_client_info().await?;

    let client_addr = resp.client_addr;

    let masked_addr = Regex::new(r"\d+")
        .unwrap()
        .replace_all(&client_addr, "1")
        .to_string();

    assert_eq!("1.1.1.1:1", masked_addr);
    Ok(())
}
