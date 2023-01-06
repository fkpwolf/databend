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

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;

pub struct TransformDummy;

impl TransformDummy {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        Transformer::create(input, output, TransformDummy {})
    }
}

#[async_trait::async_trait]
impl Transform for TransformDummy {
    const NAME: &'static str = "DummyTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        Ok(data)
    }
}
