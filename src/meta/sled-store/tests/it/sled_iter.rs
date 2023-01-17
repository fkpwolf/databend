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

use common_base::base::tokio;
use common_meta_sled_store::SledItem;
use common_meta_sled_store::SledTree;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use pretty_assertions::assert_eq;
use sled::IVec;
use testing::new_sled_test_context;

use crate::init_sled_ut;
use crate::testing;
use crate::testing::fake_key_spaces::Logs;

/// Feed some data to two trees, iterate them and check output.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_iter() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_sled_ut!();
    let _ent = ut_span.enter();

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::IncrSeq {
                    key: "foo".to_string(),
                },
            }),
        },
    ];

    tracing::info!("--- init some data");
    let t1 = {
        let tc = new_sled_test_context();

        let tree = SledTree::open(&tc.db, tc.tree_name.clone(), true)?;
        let log_tree = tree.key_space::<Logs>();

        log_tree.append(&logs).await?;
        tc.tree_name
    };

    let t2 = {
        let tc = new_sled_test_context();

        let tree = SledTree::open(&tc.db, tc.tree_name.clone(), true)?;
        let log_tree = tree.key_space::<Logs>();

        log_tree.append(&logs).await?;
        tc.tree_name
    };

    // Iterator output IVec

    let mut trees = vec![t1.clone(), t2.clone()];

    for tree_iter in common_meta_sled_store::iter::<IVec>() {
        let (tree_name, item_iter) = tree_iter?;

        if tree_name == "__sled__default" {
            continue;
        }

        if !trees.contains(&tree_name) {
            // When tests run concurrently, there are other trees created by other test case.
            continue;
        }

        assert_eq!(trees.remove(0), tree_name);

        let mut got = vec![];
        for item in item_iter {
            let (k, v) = item?;

            let item = SledItem::<Logs>::new(k, v);
            let line = format!("{}, {:?}", item.key()?, item.value()?);
            got.push(line);
        }

        let want = vec![
            "2, Entry { log_id: LogId { term: 1, index: 2 }, payload: Blank }".to_string(),
            "4, Entry { log_id: LogId { term: 3, index: 4 }, payload: Normal(LogEntry { txid: None, time_ms: None, cmd: IncrSeq { key: \"foo\" } }) }".to_string(),
        ];

        assert_eq!(want, got);
    }

    // Iterator outputs Vec<u8>

    let trees = vec![t1, t2];

    let mut got = vec![];
    for tree_iter in common_meta_sled_store::iter::<Vec<u8>>() {
        let (tree_name, item_iter) = tree_iter?;

        if !trees.contains(&tree_name) {
            // When tests run concurrently, there are other trees created by other test case.
            continue;
        }

        for item in item_iter {
            let (k, v) = item?;

            let line = format!("{:?}, {}", k, String::from_utf8(v).unwrap());
            got.push(line);
        }
    }

    let want = vec![
        "[1, 0, 0, 0, 0, 0, 0, 0, 2], {\"log_id\":{\"term\":1,\"index\":2},\"payload\":\"Blank\"}",
        "[1, 0, 0, 0, 0, 0, 0, 0, 4], {\"log_id\":{\"term\":3,\"index\":4},\"payload\":{\"Normal\":{\"txid\":null,\"cmd\":{\"IncrSeq\":{\"key\":\"foo\"}}}}}",
        "[1, 0, 0, 0, 0, 0, 0, 0, 2], {\"log_id\":{\"term\":1,\"index\":2},\"payload\":\"Blank\"}",
        "[1, 0, 0, 0, 0, 0, 0, 0, 4], {\"log_id\":{\"term\":3,\"index\":4},\"payload\":{\"Normal\":{\"txid\":null,\"cmd\":{\"IncrSeq\":{\"key\":\"foo\"}}}}}",
    ];

    assert_eq!(want, got);

    Ok(())
}
