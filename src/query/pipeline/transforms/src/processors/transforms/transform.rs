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

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

// TODO: maybe we also need async transform for `SELECT sleep(1)`?
pub trait Transform: Send {
    const NAME: &'static str;
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;

    fn name(&self) -> String {
        Self::NAME.to_string()
    }
}

pub struct Transformer<T: Transform + 'static> {
    transform: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

impl<T: Transform + 'static> Transformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Transformer {
            input,
            output,
            transform: inner,
            input_data: None,
            output_data: None,
        }))
    }
}

#[async_trait::async_trait]
impl<T: Transform + 'static> Processor for Transformer<T> {
    fn name(&self) -> String {
        Transform::name(&self.transform)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => match self.output_data.take() {
                None if self.input_data.is_some() => Ok(Event::Sync),
                None => self.pull_data(),
                Some(data) => {
                    self.output.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                }
            },
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let data_block = self.transform.transform(data_block)?;
            self.output_data = Some(data_block);
        }

        Ok(())
    }
}

impl<T: Transform> Transformer<T> {
    fn pull_data(&mut self) -> Result<Event> {
        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        self.input.finish();
        Ok(Event::Finished)
    }
}
