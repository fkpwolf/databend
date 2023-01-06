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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_storages_table_meta::meta::BlockMeta;

use crate::pruning::BlockIndex;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Deletion {
    DoNothing,
    Replaced(Arc<BlockMeta>),
    Deleted,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DeletionSourceMeta {
    pub index: BlockIndex,
    pub op: Deletion,
}

#[typetag::serde(name = "deletion_source_meta")]
impl BlockMetaInfo for DeletionSourceMeta {
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
        match info.as_any().downcast_ref::<DeletionSourceMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl DeletionSourceMeta {
    pub fn create(index: BlockIndex, op: Deletion) -> BlockMetaInfoPtr {
        Box::new(DeletionSourceMeta { index, op })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&DeletionSourceMeta> {
        match info.as_any().downcast_ref::<DeletionSourceMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to DeletionSourceMeta.",
            )),
        }
    }
}
