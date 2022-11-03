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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value as JsonValue;

use crate::servers::http::v1::json_block::block_to_json_value;
use crate::servers::http::v1::JsonBlock;
use crate::storages::result::block_buffer::BlockBuffer;

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

#[derive(Clone)]
pub struct Page {
    pub data: JsonBlock,
    pub total_rows: usize,
}

pub struct ResponseData {
    pub page: Page,
    pub next_page_no: Option<usize>,
}

pub struct PageManager {
    max_rows_per_page: usize,
    total_rows: usize,
    total_pages: usize,
    end: bool,
    block_end: bool,
    schema: DataSchemaRef,
    last_page: Option<Page>,
    row_buffer: VecDeque<Vec<JsonValue>>,
    block_buffer: Arc<BlockBuffer>,
    string_fields: bool,
    format_settings: FormatSettings,
}

impl PageManager {
    pub fn new(
        max_rows_per_page: usize,
        block_buffer: Arc<BlockBuffer>,
        string_fields: bool,
        format_settings: FormatSettings,
    ) -> PageManager {
        PageManager {
            total_rows: 0,
            last_page: None,
            total_pages: 0,
            end: false,
            block_end: false,
            row_buffer: Default::default(),
            schema: Arc::new(DataSchema::empty()),
            block_buffer,
            max_rows_per_page,
            string_fields,
            format_settings,
        }
    }

    pub fn next_page_no(&mut self) -> Option<usize> {
        if self.end {
            None
        } else {
            Some(self.total_pages)
        }
    }

    pub async fn get_a_page(&mut self, page_no: usize, tp: &Wait) -> Result<Page> {
        let next_no = self.total_pages;
        if page_no == next_no && !self.end {
            let (block, end) = self.collect_new_page(tp).await?;
            let num_row = block.num_rows();
            self.total_rows += num_row;
            let page = Page {
                data: block,
                total_rows: self.total_rows,
            };
            if num_row > 0 {
                self.total_pages += 1;
                self.last_page = Some(page.clone());
            }
            self.end = end;
            Ok(page)
        } else if page_no == next_no - 1 {
            // later, there may be other ways to ack and drop the last page except collect_new_page.
            // but for now, last_page always exists in this branch, since page_no is unsigned.
            Ok(self
                .last_page
                .as_ref()
                .ok_or_else(|| ErrorCode::Internal("last_page is None"))?
                .clone())
        } else {
            let message = format!("wrong page number {}", page_no,);
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    async fn collect_new_page(&mut self, tp: &Wait) -> Result<(JsonBlock, bool)> {
        let format_settings = &self.format_settings;
        let mut res: Vec<Vec<JsonValue>> = Vec::with_capacity(self.max_rows_per_page);
        while res.len() < self.max_rows_per_page {
            if let Some(row) = self.row_buffer.pop_front() {
                res.push(row)
            } else {
                break;
            }
        }
        loop {
            assert!(self.max_rows_per_page >= res.len());
            let remain = self.max_rows_per_page - res.len();
            if remain == 0 {
                break;
            }
            let (block, done) = self.block_buffer.pop().await?;
            match block {
                Some(block) => {
                    if self.schema.fields().is_empty() {
                        self.schema = block.schema().clone();
                    }
                    let mut iter =
                        block_to_json_value(&block, format_settings, self.string_fields)?
                            .into_iter()
                            .peekable();
                    let chunk: Vec<_> = iter.by_ref().take(remain).collect();
                    res.extend(chunk);
                    self.row_buffer = iter.by_ref().collect();
                    if done {
                        self.block_end = true;
                        break;
                    }
                }
                None => {
                    if done {
                        self.block_end = true;
                        break;
                    }
                    match tp {
                        Wait::Async => break,
                        Wait::Deadline(t) => {
                            let now = Instant::now();
                            let d = *t - now;
                            if d.is_zero()
                                || tokio::time::timeout(
                                    d,
                                    self.block_buffer.block_notify.notified(),
                                )
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    };
                }
            }
        }
        let block = JsonBlock {
            schema: self.schema.clone(),
            data: res,
        };
        if !self.block_end {
            self.block_end = self.block_buffer.is_pop_done().await;
        }
        let end = self.block_end && self.row_buffer.is_empty();
        Ok((block, end))
    }

    pub async fn detach(&self) {
        self.block_buffer.stop_pop().await;
    }
}
