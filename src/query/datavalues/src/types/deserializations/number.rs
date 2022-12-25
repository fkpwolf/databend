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

use common_exception::Result;
use common_io::prelude::BinaryRead;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;

use crate::prelude::*;
pub struct NumberDeserializer<T: PrimitiveType> {
    pub builder: MutablePrimitiveColumn<T>,
}

impl<T> TypeDeserializer for NumberDeserializer<T>
where
    T: PrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
{
    fn memory_size(&self) -> usize {
        self.builder.memory_size()
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(T::default());
    }

    fn de_fixed_binary_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_null(&mut self) -> bool {
        false
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.builder.append_data_value(value)
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
