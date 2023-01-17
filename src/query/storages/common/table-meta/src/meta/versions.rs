//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::marker::PhantomData;

use common_exception::ErrorCode;
use common_expression::DataBlock;

use super::v2;
use crate::meta::v0;
use crate::meta::v1;
use crate::meta::v2::BlockFilter;

// Here versions of meta are tagged with numeric values
//
// The trait Versioned itself can not prevent us from
// giving multiple version numbers to a specific type, such as
//
// impl Versioned<0> for v0::SegmentInfo {}
// impl Versioned<1> for v0::SegmentInfo {}
//
// Fortunately, since v0::SegmentInfo::VERSION is used in
// several places, compiler will report compile error if it
// can not deduce a unique value the constant expression.

/// Thing has a u64 version number
pub trait Versioned<const V: u64>
where Self: Sized
{
    const VERSION: u64 = V;
}

impl Versioned<0> for v0::SegmentInfo {}
impl Versioned<1> for v1::SegmentInfo {}
impl Versioned<2> for v2::SegmentInfo {}

pub enum SegmentInfoVersion {
    V0(PhantomData<v0::SegmentInfo>),
    V1(PhantomData<v1::SegmentInfo>),
    V2(PhantomData<v2::SegmentInfo>),
}

impl Versioned<0> for v0::TableSnapshot {}
impl Versioned<1> for v1::TableSnapshot {}
impl Versioned<2> for v2::TableSnapshot {}

pub enum SnapshotVersion {
    V0(PhantomData<v0::TableSnapshot>),
    V1(PhantomData<v1::TableSnapshot>),
    V2(PhantomData<v2::TableSnapshot>),
}

impl SnapshotVersion {
    pub fn version(&self) -> u64 {
        match self {
            SnapshotVersion::V0(a) => Self::ver(a),
            SnapshotVersion::V1(a) => Self::ver(a),
            SnapshotVersion::V2(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

impl Versioned<0> for v1::TableSnapshotStatistics {}

pub enum TableSnapshotStatisticsVersion {
    V0(PhantomData<v1::TableSnapshotStatistics>),
}

impl TableSnapshotStatisticsVersion {
    pub fn version(&self) -> u64 {
        match self {
            TableSnapshotStatisticsVersion::V0(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

impl Versioned<2> for DataBlock {}

pub struct V0BloomBlock {}
pub struct V2BloomBlock {}

impl Versioned<0> for V0BloomBlock {}
impl Versioned<2> for V2BloomBlock {}

impl Versioned<3> for BlockFilter {}

pub enum BlockBloomFilterIndexVersion {
    V0(PhantomData<V0BloomBlock>),
    V2(PhantomData<V2BloomBlock>),
    V3(PhantomData<v2::BlockFilter>),
}

mod converters {

    use super::*;

    impl TryFrom<u64> for SegmentInfoVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(SegmentInfoVersion::V0(
                    // `ver_eq<_, 0>` is merely a static check to make sure that
                    // the type `v0::SegmentInfoVersion` do have a numeric version number of 0
                    ver_eq::<_, 0>(PhantomData),
                )),
                1 => Ok(SegmentInfoVersion::V1(ver_eq::<_, 1>(PhantomData))),
                2 => Ok(SegmentInfoVersion::V2(ver_eq::<_, 2>(PhantomData))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    impl TryFrom<u64> for SnapshotVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(SnapshotVersion::V0(ver_eq::<_, 0>(PhantomData))),
                1 => Ok(SnapshotVersion::V1(ver_eq::<_, 1>(PhantomData))),
                2 => Ok(SnapshotVersion::V2(ver_eq::<_, 2>(PhantomData))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown snapshot segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    impl TryFrom<u64> for TableSnapshotStatisticsVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(TableSnapshotStatisticsVersion::V0(ver_eq::<_, 0>(
                    PhantomData,
                ))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown table snapshot statistics version {value}, versions supported: 0"
                ))),
            }
        }
    }

    impl TryFrom<u64> for BlockBloomFilterIndexVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                1 => Err(ErrorCode::DeprecatedIndexFormat(
                    "v1 bloom filter index is deprecated",
                )),
                // version 2 and version 3 are using the same StringColumn to storage the bloom filter
                2 => Ok(BlockBloomFilterIndexVersion::V2(ver_eq::<_, 2>(
                    PhantomData,
                ))),
                3 => Ok(BlockBloomFilterIndexVersion::V3(ver_eq::<_, 3>(
                    PhantomData,
                ))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown block bloom filer index version {value}, versions supported: 1"
                ))),
            }
        }
    }

    /// Statically check that if T implements Versoined<U> where U equals V
    #[inline]
    fn ver_eq<T, const V: u64>(t: PhantomData<T>) -> PhantomData<T>
    where T: Versioned<V> {
        t
    }
}
