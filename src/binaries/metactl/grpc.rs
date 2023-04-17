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

use std::fs::File;
use std::io::Write;
use std::time::Duration;

use common_meta_client::MetaGrpcClient;
use common_meta_types::protobuf::Empty;
use tokio_stream::StreamExt;

pub async fn export_meta(addr: &str, save: String) -> anyhow::Result<()> {
    let client = MetaGrpcClient::try_create(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        None,
        Duration::from_secs(10),
        None,
    )?;

    let mut grpc_client = client.make_client().await?;

    let exported = grpc_client.export(tonic::Request::new(Empty {})).await?;

    let mut stream = exported.into_inner();

    let file: Option<File> = if !save.is_empty() {
        Some(File::create(&save)?)
    } else {
        None
    };

    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res?;

        for line in &chunk.data {
            if file.as_ref().is_none() {
                println!("{}", line);
            } else {
                file.as_ref()
                    .unwrap()
                    .write_all(format!("{}\n", line).as_bytes())?;
            }
        }
    }

    if file.as_ref().is_some() {
        file.as_ref().unwrap().sync_all()?;
    }

    Ok(())
}
