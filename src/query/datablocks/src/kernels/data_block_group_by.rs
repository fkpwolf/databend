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

use common_datavalues::remove_nullable;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_exception::Result;

use crate::kernels::HashMethodKeysU16;
use crate::kernels::HashMethodKeysU32;
use crate::kernels::HashMethodKeysU64;
use crate::kernels::HashMethodKeysU8;
use crate::kernels::HashMethodKind;
use crate::kernels::HashMethodSerializer;
use crate::DataBlock;
use crate::HashMethodKeysU128;
use crate::HashMethodKeysU256;
use crate::HashMethodKeysU512;

impl DataBlock {
    pub fn choose_hash_method(block: &DataBlock, indices: &[usize]) -> Result<HashMethodKind> {
        let hash_key_types = indices
            .iter()
            .map(|&c| {
                let col = block.column(c);
                Ok(col.data_type())
            })
            .collect::<Result<Vec<_>>>();

        let hash_key_types = hash_key_types?;
        Self::choose_hash_method_with_types(&hash_key_types)
    }

    pub fn choose_hash_method_with_types(
        hash_key_types: &[DataTypeImpl],
    ) -> Result<HashMethodKind> {
        let mut group_key_len = 0;
        for typ in hash_key_types {
            let not_null_type = remove_nullable(typ);

            if not_null_type.data_type_id().is_numeric()
                || not_null_type.data_type_id().is_date_or_date_time()
            {
                group_key_len += not_null_type.data_type_id().numeric_byte_size()?;

                // extra one byte for null flag
                if typ.is_nullable() {
                    group_key_len += 1;
                }
            } else {
                return Ok(HashMethodKind::Serializer(HashMethodSerializer::default()));
            }
        }

        match group_key_len {
            1 => Ok(HashMethodKind::KeysU8(HashMethodKeysU8::default())),
            2 => Ok(HashMethodKind::KeysU16(HashMethodKeysU16::default())),
            3..=4 => Ok(HashMethodKind::KeysU32(HashMethodKeysU32::default())),
            5..=8 => Ok(HashMethodKind::KeysU64(HashMethodKeysU64::default())),
            9..=16 => Ok(HashMethodKind::KeysU128(HashMethodKeysU128::default())),
            17..=32 => Ok(HashMethodKind::KeysU256(HashMethodKeysU256::default())),
            33..=64 => Ok(HashMethodKind::KeysU512(HashMethodKeysU512::default())),
            _ => Ok(HashMethodKind::Serializer(HashMethodSerializer::default())),
        }
    }
}
