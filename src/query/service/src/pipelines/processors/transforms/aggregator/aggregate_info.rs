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

use std::any::Any;

use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AggregateInfo {
    pub bucket: isize,
}

impl AggregateInfo {
    pub fn create(bucket: isize) -> BlockMetaInfoPtr {
        Box::new(AggregateInfo { bucket })
    }
}

#[typetag::serde(name = "aggregate_info")]
impl BlockMetaInfo for AggregateInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<AggregateInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
