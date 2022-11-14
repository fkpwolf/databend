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

use common_datavalues::prelude::*;
use serde_json::json;
use serde_json::Value as JsonValue;

#[test]
fn test_empty_object_column() {
    let mut builder = MutableObjectColumn::<VariantValue>::with_capacity(16);
    let data_column: ObjectColumn<VariantValue> = builder.finish();
    let mut iter = data_column.iter();
    assert_eq!(None, iter.next());
    assert!(data_column.is_empty());
}

#[test]
fn test_new_from_slice() {
    let a = VariantValue::from(JsonValue::Bool(true));
    let b = VariantValue::from(JsonValue::Bool(false));
    let v = vec![&a, &b];
    let data_column: ObjectColumn<VariantValue> = VariantColumn::from_slice(v.as_slice());
    let mut iter = data_column.iter();
    assert_eq!(Some(&a), iter.next());
    assert_eq!(Some(&b), iter.next());
    assert_eq!(None, iter.next());

    data_column.for_each(|i, data| {
        assert_eq!(DataValue::Variant(v[i].to_owned()), data);
    })
}

#[test]
fn test_object_column() {
    const N: usize = 1024;
    let a = VariantValue::from(json!(true));
    let b = VariantValue::from(json!(false));
    let it = (0..N).map(|i| if i % 2 == 0 { &a } else { &b });
    let data_column: ObjectColumn<VariantValue> = VariantColumn::from_iterator(it);
    assert!(!data_column.is_empty());
    assert!(data_column.len() == N);
    assert!(!data_column.null_at(1));

    assert!(data_column.get(512) == DataValue::Variant(VariantValue::from(json!(true))));
    assert!(data_column.get(513) == DataValue::Variant(VariantValue::from(json!(false))));

    let slice = data_column.slice(0, N / 2);
    assert!(slice.len() == N / 2);
}
