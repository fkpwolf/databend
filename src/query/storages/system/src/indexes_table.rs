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

use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::ListIndexesReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_storages_fuse::TableContext;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct IndexesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for IndexesTable {
    const NAME: &'static str = "system.indexes";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
        let indexes = catalog
            .list_indexes(ListIndexesReq {
                tenant,
                table_id: None,
            })
            .await?;

        let mut names = Vec::with_capacity(indexes.len());
        let mut types = Vec::with_capacity(indexes.len());
        let mut defs = Vec::with_capacity(indexes.len());
        let mut created_on = Vec::with_capacity(indexes.len());

        for (_, name, index) in indexes {
            names.push(name.as_bytes().to_vec());
            types.push(index.index_type.to_string().as_bytes().to_vec());
            defs.push(index.query.as_bytes().to_vec());
            created_on.push(index.created_on.timestamp_micros());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            StringType::from_data(defs),
            TimestampType::from_data(created_on),
        ]))
    }
}

impl IndexesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("definition", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'indexes'".to_string(),
            name: "indexes".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemIndexes".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
