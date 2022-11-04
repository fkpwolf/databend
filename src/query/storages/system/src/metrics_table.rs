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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_metrics::MetricValue;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct MetricsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for MetricsTable {
    const NAME: &'static str = "system.metrics";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        let prometheus_handle = common_metrics::try_handle().ok_or_else(|| {
            ErrorCode::InitPrometheusFailure("Prometheus recorder is not initialized yet.")
        })?;

        let samples = common_metrics::dump_metric_samples(prometheus_handle)?;
        let mut metrics: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut labels: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut kinds: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        for sample in samples.into_iter() {
            metrics.push(sample.name.clone().into_bytes());
            kinds.push(sample.kind.clone().into_bytes());
            labels.push(self.display_sample_labels(&sample.labels)?.into_bytes());
            values.push(self.display_sample_value(&sample.value)?.into_bytes());
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(metrics),
            Series::from_data(kinds),
            Series::from_data(labels),
            Series::from_data(values),
        ]))
    }
}

impl MetricsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("metric", Vu8::to_data_type()),
            DataField::new("kind", Vu8::to_data_type()),
            DataField::new("labels", Vu8::to_data_type()),
            DataField::new("value", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'metrics'".to_string(),
            name: "metrics".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemMetrics".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(MetricsTable { table_info })
    }

    fn display_sample_labels(&self, labels: &HashMap<String, String>) -> Result<String> {
        serde_json::to_string(labels).map_err(|err| {
            ErrorCode::Internal(format!(
                "Dump prometheus metrics on display labels: {}",
                err
            ))
        })
    }

    fn display_sample_value(&self, value: &MetricValue) -> Result<String> {
        match value {
            MetricValue::Counter(v) => serde_json::to_string(v),
            MetricValue::Gauge(v) => serde_json::to_string(v),
            MetricValue::Untyped(v) => serde_json::to_string(v),
            MetricValue::Histogram(v) => serde_json::to_string(v),
            MetricValue::Summary(v) => serde_json::to_string(v),
        }
        .map_err(|err| {
            ErrorCode::Internal(format!(
                "Dump prometheus metrics failed on display values: {}",
                err
            ))
        })
    }
}
