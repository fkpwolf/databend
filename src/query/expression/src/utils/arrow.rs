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

use std::io::Cursor;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::ipc::read::read_file_metadata;
use common_arrow::arrow::io::ipc::read::FileReader;
use common_arrow::arrow::io::ipc::write::FileWriter;
use common_arrow::arrow::io::ipc::write::WriteOptions;

pub fn bitmap_into_mut(bitmap: Bitmap) -> MutableBitmap {
    bitmap
        .into_mut()
        .map_left(|bitmap| {
            let mut builder = MutableBitmap::new();
            builder.extend_from_bitmap(&bitmap);
            builder
        })
        .into_inner()
}

pub fn repeat_bitmap(bitmap: &mut Bitmap, n: usize) -> MutableBitmap {
    let mut builder = MutableBitmap::new();
    for _ in 0..n {
        builder.extend_from_bitmap(bitmap);
    }
    builder
}

pub fn append_bitmap(bitmap: &mut MutableBitmap, other: &Bitmap) {
    bitmap.extend_from_bitmap(other)
}

pub fn constant_bitmap(value: bool, len: usize) -> MutableBitmap {
    let mut builder = MutableBitmap::new();
    builder.extend_constant(len, value);
    builder
}

pub fn buffer_into_mut<T: Clone>(mut buffer: Buffer<T>) -> Vec<T> {
    buffer
        .get_mut()
        .map(std::mem::take)
        .unwrap_or_else(|| buffer.to_vec())
}

pub fn serialize_arrow_array(col: Box<dyn Array>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let schema = Schema::from(vec![Field::new("col", col.data_type().clone(), true)]);
    let mut writer = FileWriter::new(&mut buffer, schema, None, WriteOptions::default());
    writer.start().unwrap();
    writer
        .write(&common_arrow::arrow::chunk::Chunk::new(vec![col]), None)
        .unwrap();
    writer.finish().unwrap();
    buffer
}

pub fn deserialize_arrow_array(bytes: &[u8]) -> Option<Box<dyn Array>> {
    let mut cursor = Cursor::new(bytes);
    let metadata = read_file_metadata(&mut cursor).ok()?;
    let mut reader = FileReader::new(cursor, metadata, None, None);
    let col = reader.next()?.ok()?.into_arrays().remove(0);
    Some(col)
}
