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

use std::f64::consts::PI;

use common_datavalues::prelude::*;

#[test]
fn test_empty_primitive_column() {
    let mut builder = MutablePrimitiveColumn::<i32>::with_capacity(16);
    let data_column: PrimitiveColumn<i32> = builder.finish();
    let mut iter = data_column.iter();
    assert_eq!(None, iter.next());
    assert!(data_column.is_empty());
}

#[test]
fn test_new_from_slice() {
    let values = &[1, 2];
    let data_column: PrimitiveColumn<i32> = Int32Column::from_slice(values);
    let mut iter = data_column.iter();
    assert_eq!(Some(&1), iter.next());
    assert_eq!(Some(&2), iter.next());
    assert_eq!(None, iter.next());

    data_column.for_each(|i, data| {
        assert_eq!(DataValue::Int64(values[i] as i64), data);
    })
}

#[test]
fn test_primitive_column() {
    const N: usize = 1024;
    let it = (0..N).map(|i| i as i32);
    let data_column: PrimitiveColumn<i32> = Int32Column::from_iterator(it);
    assert!(!data_column.is_empty());
    assert!(data_column.len() == N);
    assert!(!data_column.null_at(1));

    assert!(data_column.get_i64(512).unwrap() == 512);

    let slice = data_column.slice(0, N / 2);
    assert!(slice.len() == N / 2);
}

#[test]
fn test_const_column() {
    let c = ConstColumn::new(Series::from_data(vec![PI]), 24).arc();
    println!("{:?}", c);
}

#[test]
fn test_filter_column() {
    const N: usize = 1000;
    let it = (0..N).map(|i| i as i32);
    let data_column: PrimitiveColumn<i32> = Int32Column::from_iterator(it);

    struct Test {
        filter: BooleanColumn,
        expect: Vec<i32>,
        deleted: Option<Vec<i32>>,
    }

    let mut tests: Vec<Test> = vec![
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|_| true)),
            expect: (0..N).map(|i| i as i32).collect(),
            deleted: None,
        },
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|_| false)),
            expect: vec![],
            deleted: Some((0..N).map(|i| i as i32).collect()),
        },
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|i| i % 10 == 0)),
            expect: (0..N).map(|i| i as i32).filter(|i| i % 10 == 0).collect(),
            deleted: Some((0..N).map(|i| i as i32).filter(|i| i % 10 != 0).collect()),
        },
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|i| !(100..=800).contains(&i))),
            expect: (0..N)
                .map(|i| i as i32)
                .filter(|&i| !(100..=800).contains(&i))
                .collect(),
            deleted: Some(
                (0..N)
                    .map(|i| i as i32)
                    .filter(|&i| (100..=800).contains(&i))
                    .collect(),
            ),
        },
    ];

    let offset = 10;
    let filter = BooleanColumn::from_iterator(
        (0..N + offset).map(|i| !(100 + offset..=800 + offset).contains(&i)),
    )
    .slice(offset, N)
    .as_any()
    .downcast_ref::<BooleanColumn>()
    .unwrap()
    .clone();
    tests.push(Test {
        filter,
        expect: (0..N)
            .map(|i| i as i32)
            .filter(|&i| !(100..=800).contains(&i))
            .collect(),
        deleted: Some(
            (0..N)
                .map(|i| i as i32)
                .filter(|&i| (100..=800).contains(&i))
                .collect(),
        ),
    });

    for test in tests {
        let res = data_column.filter(&test.filter);
        assert_eq!(
            res.as_any()
                .downcast_ref::<PrimitiveColumn<i32>>()
                .unwrap()
                .values(),
            test.expect
        );

        if let Some(deleted) = test.deleted {
            let res = data_column.filter(&test.filter.neg());
            assert_eq!(
                res.as_any()
                    .downcast_ref::<PrimitiveColumn<i32>>()
                    .unwrap()
                    .values(),
                deleted
            );
        }
    }
}
