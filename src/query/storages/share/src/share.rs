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

use std::collections::BTreeMap;

use common_exception::Result;
use common_meta_app::share::ShareDatabaseSpec;
use common_meta_app::share::ShareSpec;
use common_meta_app::share::ShareTableSpec;
use opendal::Operator;

pub const SHARE_CONFIG_PREFIX: &str = "_share_config";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpecVec {
    share_specs: BTreeMap<String, ext::ShareSpecExt>,
}

pub async fn save_share_spec(
    tenant: &String,
    operator: Operator,
    spec_vec: Option<Vec<ShareSpec>>,
) -> Result<()> {
    if let Some(spec_vec) = spec_vec {
        let location = format!("{}/{}/share_specs.json", tenant, SHARE_CONFIG_PREFIX);
        let mut share_spec_vec = ShareSpecVec::default();
        for spec in spec_vec {
            let share_name = spec.name.clone();
            let share_spec_ext = ext::ShareSpecExt::from_share_spec(spec, &operator);
            share_spec_vec
                .share_specs
                .insert(share_name, share_spec_ext);
        }
        operator
            .object(&location)
            .write(serde_json::to_vec(&share_spec_vec)?)
            .await?;
    }

    Ok(())
}

mod ext {
    use storages_common_table_meta::table::database_storage_prefix;
    use storages_common_table_meta::table::table_storage_prefix;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    struct WithLocation<T> {
        location: String,
        #[serde(flatten)]
        t: T,
    }

    /// An extended form of [ShareSpec], which decorates [ShareDatabaseSpec] and [ShareTableSpec]
    /// with location
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    pub(super) struct ShareSpecExt {
        name: String,
        share_id: u64,
        version: u64,
        database: Option<WithLocation<ShareDatabaseSpec>>,
        tables: Vec<WithLocation<ShareTableSpec>>,
        tenants: Vec<String>,
    }

    impl ShareSpecExt {
        pub fn from_share_spec(spec: ShareSpec, operator: &Operator) -> Self {
            Self {
                name: spec.name,
                share_id: spec.share_id,
                version: spec.version,
                database: spec.database.map(|db_spec| WithLocation {
                    location: shared_database_prefix(operator, db_spec.id),
                    t: db_spec,
                }),
                tables: spec
                    .tables
                    .into_iter()
                    .map(|tbl_spec| WithLocation {
                        location: shared_table_prefix(
                            operator,
                            tbl_spec.database_id,
                            tbl_spec.table_id,
                        ),
                        t: tbl_spec,
                    })
                    .collect(),
                tenants: spec.tenants,
            }
        }
    }

    /// Returns prefix path which covers all the data of give table.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/501263/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    ///   - "501263/" is the table id  suffixed with '/'
    fn shared_table_prefix(operator: &Operator, database_id: u64, table_id: u64) -> String {
        let operator_meta_data = operator.metadata();
        let storage_prefix = operator_meta_data.root();
        let table_storage_prefix = table_storage_prefix(database_id, table_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, table_storage_prefix)
    }

    /// Returns prefix path which covers all the data of give database.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    fn shared_database_prefix(operator: &Operator, database_id: u64) -> String {
        let operator_meta_data = operator.metadata();
        let storage_prefix = operator_meta_data.root();
        let database_storage_prefix = database_storage_prefix(database_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, database_storage_prefix)
    }

    #[cfg(test)]
    mod tests {

        use opendal::services::Fs;
        use opendal::Builder;

        use super::*;

        #[test]
        fn test_serialize_share_spec_ext() -> Result<()> {
            let share_spec = ShareSpec {
                name: "test_share_name".to_string(),
                version: 1,
                share_id: 1,
                database: Some(ShareDatabaseSpec {
                    name: "share_database".to_string(),
                    id: 1,
                }),
                tables: vec![ShareTableSpec {
                    name: "share_table".to_string(),
                    database_id: 1,
                    table_id: 1,
                    presigned_url_timeout: "100s".to_string(),
                }],
                tenants: vec!["test_tenant".to_owned()],
            };
            let tmp_dir = tempfile::tempdir()?;
            let test_root = tmp_dir.path().join("test_cluster_id/test_tenant_id");
            let test_root_str = test_root.to_str().unwrap();
            let operator = {
                let mut builder = Fs::default();
                builder.root(test_root_str);
                Operator::new(builder.build()?).finish()
            };

            let share_spec_ext = ShareSpecExt::from_share_spec(share_spec, &operator);
            let spec_json_value = serde_json::to_value(share_spec_ext).unwrap();

            use serde_json::json;

            let expected = json!({
              "name": "test_share_name",
              "version": 1,
              "share_id": 1,
              "database": {
                "location": format!("{}/1/", test_root_str),
                "name": "share_database",
                "id": 1
              },
              "tables": [
                {
                  "location": format!("{}/1/1/", test_root_str),
                  "name": "share_table",
                  "database_id": 1,
                  "table_id": 1,
                  "presigned_url_timeout": "100s"
                }
              ],
              "tenants": [
                "test_tenant"
              ]
            });

            assert_eq!(expected, spec_json_value);
            Ok(())
        }
    }
}
