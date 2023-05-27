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

use std::fmt;

use crate::ondisk::version_info::VersionInfo;
use crate::ondisk::version_info::VERSION_INFOS;

/// Available data versions this program can work upon.
///
/// It is store in a standalone `sled::Tree`. In this tree there are two `DataVersion` record: the current version of the on-disk data, and the version to upgrade to.
/// The `upgrading` is `Some` only when the upgrading progress is shut down before finishing.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum DataVersion {
    /// The first version.
    /// The Data is compatible with openraft v07 and v08, using openraft::compat.
    V0,
    /// Get rid of compat, use only openraft v08 data types.
    V001,
}

impl fmt::Debug for DataVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataVersion::V0 => write!(
                f,
                "V0(2023-04-21: compatible with openraft v07 and v08, using openraft::compat)"
            ),
            DataVersion::V001 => write!(
                f,
                "V001(2023-05-15: Get rid of compat, use only openraft v08 data types)"
            ),
        }
    }
}

impl fmt::Display for DataVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataVersion::V0 => write!(f, "V0"),
            DataVersion::V001 => write!(f, "V001"),
        }
    }
}

impl DataVersion {
    /// Returns the version immediately following this one.
    pub fn next(&self) -> Option<Self> {
        match self {
            Self::V0 => Some(Self::V001),
            Self::V001 => None,
        }
    }

    /// Check if the on-disk data is compatible with this version.
    pub fn is_compatible(&self, on_disk: Self) -> bool {
        self.min_compatible_data_version() <= on_disk && on_disk <= *self
    }

    /// Return the minimal on-disk version it can work with.
    pub fn min_compatible_data_version(&self) -> Self {
        match self {
            Self::V0 => Self::V0,
            Self::V001 => Self::V0,
        }
    }

    /// Return the maximal working data version that can work with this version.
    pub fn max_compatible_working_version(&self) -> Self {
        let mut working_version = *self;

        while let Some(next) = working_version.next() {
            if next.is_compatible(*self) {
                working_version = next;
            } else {
                break;
            }
        }

        working_version
    }

    /// Get administrative information for upgrading and compatibility.
    pub fn version_info(&self) -> VersionInfo {
        VERSION_INFOS.get(self).unwrap().clone()
    }
}
