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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use snailquote::escape;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct SettingsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for SettingsTable {
    const NAME: &'static str = "system.settings";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let settings = ctx.get_settings().get_setting_values();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut defaults: Vec<String> = vec![];
        let mut levels: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        for vals in settings {
            // Name.
            names.push(vals.0);
            // Value.
            values.push(escape(format!("{:?}", vals.1).as_str()).to_string());
            // Default Value.
            defaults.push(escape(format!("{:?}", vals.2).as_str()).to_string());
            // Scope level.
            levels.push(vals.3);
            // Desc.
            descs.push(vals.4);

            let typename = match vals.2 {
                common_meta_types::UserSettingValue::UInt64(_) => "UInt64",
                common_meta_types::UserSettingValue::String(_) => "String",
            };
            // Types.
            types.push(typename.to_string());
        }

        let names: Vec<&[u8]> = names.iter().map(|x| x.as_bytes()).collect();
        let values: Vec<&[u8]> = values.iter().map(|x| x.as_bytes()).collect();
        let defaults: Vec<&[u8]> = defaults.iter().map(|x| x.as_bytes()).collect();
        let levels: Vec<&[u8]> = levels.iter().map(|x| x.as_bytes()).collect();
        let descs: Vec<&[u8]> = descs.iter().map(|x| x.as_bytes()).collect();
        let types: Vec<&[u8]> = types.iter().map(|x| x.as_bytes()).collect();

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(values),
            Series::from_data(defaults),
            Series::from_data(levels),
            Series::from_data(descs),
            Series::from_data(types),
        ]))
    }
}

impl SettingsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("value", Vu8::to_data_type()),
            DataField::new("default", Vu8::to_data_type()),
            DataField::new("level", Vu8::to_data_type()),
            DataField::new("description", Vu8::to_data_type()),
            DataField::new("type", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'settings'".to_string(),
            name: "settings".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemSettings".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(SettingsTable { table_info })
    }
}
