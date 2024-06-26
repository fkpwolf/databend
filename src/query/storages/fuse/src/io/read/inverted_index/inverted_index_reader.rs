// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_expression::DataSchema;
use databend_storages_common_index::InvertedIndexDirectory;
use opendal::Operator;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::Index;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_filter;
use crate::io::write::create_tokenizer_manager;

#[derive(Clone)]
pub struct InvertedIndexReader {
    fields: Vec<Field>,
    directory: Arc<InvertedIndexDirectory>,
}

impl InvertedIndexReader {
    pub async fn try_create(
        dal: Operator,
        schema: &DataSchema,
        query_columns: &Vec<String>,
        index_loc: &str,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(query_columns.len());
        for column_name in query_columns {
            let i = schema.index_of(column_name)?;
            let field = Field::from_field_id(i as u32);
            fields.push(field);
        }

        let directory = load_inverted_index_filter(dal.clone(), index_loc.to_string()).await?;

        Ok(Self { fields, directory })
    }

    // Filter the rows and scores in the block that can match the query text,
    // if there is no row that can match, this block can be pruned.
    pub fn do_filter(&self, query: &str, row_count: u64) -> Result<Option<Vec<(usize, F32)>>> {
        let tokenizer_manager = create_tokenizer_manager();
        let directory = Arc::unwrap_or_clone(self.directory.clone());
        let mut index = Index::open(directory)?;
        index.set_tokenizers(tokenizer_manager.clone());
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let query_parser = QueryParser::for_index(&index, self.fields.clone());
        let query = query_parser.parse_query(query)?;

        let collector = TopDocs::with_limit(row_count as usize);
        let docs = searcher.search(&query, &collector)?;

        if docs.is_empty() {
            return Ok(None);
        }

        let mut matched_rows = Vec::new();
        for (score, doc_addr) in docs {
            let doc_id = doc_addr.doc_id as usize;
            let score = F32::from(score);
            matched_rows.push((doc_id, score));
        }

        Ok(Some(matched_rows))
    }
}
